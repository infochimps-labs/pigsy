package com.infochimps.hadoop.pig.geo;

import java.io.IOException;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import java.util.Collections;

import org.apache.commons.codec.digest.DigestUtils;

import org.mapfish.geo.MfGeo;
import org.mapfish.geo.MfGeoFactory;
import org.mapfish.geo.MfGeoJSONReader;
import org.mapfish.geo.MfGeometry;
import org.mapfish.geo.MfFeature;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.backend.executionengine.ExecException;

import com.vividsolutions.jts.geom.*;
import com.vividsolutions.jts.algorithm.CentroidPoint;

import org.json.JSONException;
import org.json.JSONObject;

/**
   
   Given a tile quadkey and a bag of points, runs K-means clustering over the points.
   Note that if the DispatchPoints UDF was used (and it _should_ be used here) then
   the bag of points will contain not only the points for the tile specified by the
   quadkey but also those of its neighbors. This is to capture clusters near the
   edges of tiles properly.
   <p>
   Arguments:
   <ul>
   <li><b>numCenters</b>: The number of centers (K) to use for K-means clustering</li>
   <li><b>qualifier</b>: The namespace + '.' + protocol + '.' + type that qualifies the input points
   <li><b>quadkey</b>: A google quadkey representing a tile.</li>
   <li><b>bag_of_points</b>: A bag of geoJSON points containing the set of points that map to the tile
   specified by the quadkey given _as well as_ the neigboring tiles.
   </ul>
   <p>
   Result:
   A Pig 'Map' object which can have <b>exactly one</b> of the following keys:
   <ul>
   <li><b>points</b>: A Pig bag containing only the points inside the tile. This happens
   when the number of points passed in is less than the maximum points allowed per tile.</li>
   <li><b>clusters</b>:A Pig bag containging only the clusters inside the tile. This happens
   when the number of points passed in is greater than the maximum points allowed per tile.</li>
   </ul>
      
 */
public class SummarizeTile extends EvalFunc<DataBag> {

    private static TupleFactory tupleFactory = TupleFactory.getInstance();
    private static BagFactory   bagFactory = BagFactory.getInstance();

    // The maximum number of points to allow a tile to have before clusters are generated
    private static final int MAX_POINTS_PER_TILE = 256;
    
    private static final String CLUSTERS = "clusters";
    private static final String POINTS = "points";

    private final String COLON = ":";
    private final String PERIOD = ".";
    private final String CHARSET = "UTF-8";
    
    // The keys to use in the geoJSON serialization of a cluster. Indicates the number of points used.
    private static final String CLUSTER_KEY = "_total";
    private static final String CHILDREN_KEY = "children";
    private static final String INSIDE_TILE = "inside_tile";
    private static final String TYPE_KEY = "_type";
    private static final String CLUSTER_TYPE = "cluster_point";
    
    // Simple factory for creating geoJSON features from json strings
    private final MfGeoFactory mfFactory = new MfGeoFactory() {
            public MfFeature createFeature(String id, MfGeometry geometry, JSONObject properties) {
                return new GeoFeature(id, geometry, properties);
            }
        };

    private static final GeometryFactory geomFactory = new GeometryFactory();
    private final MfGeoJSONReader reader = new MfGeoJSONReader(mfFactory);
    
    public DataBag exec(Tuple input) throws IOException {
        if (input == null || input.size() < 4)
            return null;

        DataBag result = bagFactory.newDefaultBag(); // Result

        // Get arguments from the input tuple
        Integer numCenters = (Integer)input.get(0);
        String qualifier = input.get(1).toString();         // namespace.protocol.type
        String quadKey = input.get(2).toString();
        DataBag points = (DataBag)input.get(3);

        // Convert the tile quadkey into a bounding box object
        Polygon space = QuadKeyUtils.quadKeyToBox(quadKey); 

        // Read ALL of the points into memory and clear the bag.
        List<GeoFeature> pointList = bagToList(points); 
        points.clear();                                 

        // If there aren't enough points then don't bother clustering and only return points inside the tile
        if (pointList.size() < MAX_POINTS_PER_TILE) { 
            List<GeoFeature> inside = pointsWithin(space, pointList);
            result = listToBag(inside);
        } else {

            // Get initial set of K centers
            List<GeoFeature> kCenters = getKCenters(qualifier, quadKey, pointList, numCenters);

            //
            System.out.println(kCenters.size());
            //
            
            // Remove the centers from the list of points to include in the calculation
            for (GeoFeature center : kCenters) pointList.remove(center);

            // Create two hashmaps to hold center ids and the lists of points associated with them
            HashMap<String, List<GeoFeature>> currentCenters = initNewCenters(kCenters);
            HashMap<String, List<GeoFeature>> newCenters = initNewCenters(kCenters);

            // Compute the similarity. This should be 1.0 at the beginning
            double sim = similarity(currentCenters, newCenters);
            
            //
            // Iterate _at most_ 100 times for k-means
            //
            for (int i = 0; i < 100; i++) {

                // Get information about the first center as a starting point for the calculation
                GeoFeature firstCenter = kCenters.get(0);
                String firstCenterId = firstCenter.getFeatureId();
                Point firstCenterPoint = (Point)firstCenter.getMfGeometry().getInternalGeometry();

                for (GeoFeature point : pointList) {
                    
                    Point geoPoint = (Point)point.getMfGeometry().getInternalGeometry();
                    double distance = geoPoint.distance(firstCenterPoint); // compute distance to first center
                    String centerId = firstCenterId;                       // associate 'point' with the first center

                    // Find nearest center
                    for (GeoFeature center : kCenters) {
                        Point centerPoint = (Point)center.getMfGeometry().getInternalGeometry();
                        double distanceToCenter = geoPoint.distance(centerPoint);
                        if (distanceToCenter < distance) {
                            distance = distanceToCenter;
                            centerId = center.getFeatureId();
                        }   
                    }

                    // Add the point to the list for its nearest center
                    Object centerList = newCenters.get(centerId);
                    if (centerList != null) {
                        List<GeoFeature> nearestCentersPoints = (List<GeoFeature>)centerList;
                        nearestCentersPoints.add(point);
                        newCenters.put(centerId, nearestCentersPoints);
                    } else {
                        List<GeoFeature> nearestCentersPoints = new ArrayList<GeoFeature>();
                        nearestCentersPoints.add(point);
                        newCenters.put(centerId, nearestCentersPoints);
                    }
                }

                // Compute the similarity between the currentCenters and the latest newCenters
                sim = similarity(currentCenters, newCenters);
                // Report progress to Hadoop which iteration we're on
                reporter.progress("Iteration ["+i+"], convergence ["+sim+"]");

                // System.out.println("New centers:");
                // printCenters(newCenters);
                // System.out.println("Current centers:");
                // printCenters(currentCenters);
                
                // Break if the new centers are the same as the old centers
                if (sim >= 0.99) break;

                // Update K centers to be centroids
                kCenters = computeCentroids(space, newCenters);


                // copy new centers to current centers
                currentCenters = (HashMap<String, List<GeoFeature>>)newCenters.clone();
                currentCenters.putAll(newCenters);

                // Nuke everything in the newCenters HashMap and start again
                newCenters = initNewCenters(kCenters);
            }

            // FIXME: Only return clusters that have at least one point inside the tile
            List<GeoFeature> finalCenters = filterCentersByInsideTile(kCenters);
            result = listToBag(finalCenters);
        }
        return result;
    }

    private void printCenters(HashMap<String, List<GeoFeature>> centers) {
        for (Map.Entry<String,List<GeoFeature>> entry : centers.entrySet()) {
            System.out.println("Center ["+entry.getKey().toString()+"]");
            List<GeoFeature> points = (List<GeoFeature>)entry.getValue();

            
            for (GeoFeature point : points) {
                System.out.println("     child ["+point.serialize()+"]");
            }

            if (points.size()==1) {
                System.out.println("     Centroid ["+points.get(0).serialize()+"]");
            } else {
                List<String> children = new ArrayList<String>(points.size()); // Will hold a center's children ids

                // Add some metadata about the children to the center
                JSONObject metaData = new JSONObject();

                // Add all the points associated with a center to the 'CentroidPoint' object
                int numPoints = 0;
                CentroidPoint c = new CentroidPoint();
                for (GeoFeature point : points) {
                    try {
                        // If the child point is a cluster itself, need to accumulate the number of points it has as well
                        boolean isCluster = false;
                        if ((point.getProperties().has(TYPE_KEY) && point.getProperties().getString(TYPE_KEY).equals(CLUSTER_TYPE)) || point.getProperties().has(CLUSTER_KEY)) isCluster = true;
                    
                        if (isCluster) {
                            numPoints += point.getProperties().getInt(CLUSTER_KEY);
                        }
                        // add child id to the children array if it isn't the id of the parent
                        if (!point.getFeatureId().equals(entry.getKey().toString())) children.add(point.getFeatureId()); 
                
                        Point geoPoint = (Point)point.getMfGeometry().getInternalGeometry();

                        if (numPoints==0) numPoints = points.size();
                    
                        metaData.put(CLUSTER_KEY, numPoints);
                        c.add(geoPoint);
                    } catch (JSONException e) {
                        e.printStackTrace();
                    }
                }

                try {
                    if (children.size() > 0) metaData.put(CHILDREN_KEY, children); // eg, {"children":['8u9qhjncna90ah', 'jfkah8034ri9z', ...]}
                } catch (JSONException e) {
                    e.printStackTrace();
                }
                        
                // create a new point object from the centroid and add to the centroids list
                Point jtsP = geomFactory.createPoint(c.getCentroid()); 
                MfGeometry mfP = new MfGeometry(jtsP);
                GeoFeature featureP = new GeoFeature(entry.getKey().toString(), mfP, metaData);

                System.out.println("     Centroid ["+featureP.serialize()+"]");
            }
            System.out.println("---");
        }
    }
    
    /**
       Given a DataBag containing serialized geoJSON points will return a List containing
       the de-serialized representations of these points. The primary issue here, of course,
       is that all the points must be read into memory.
     */
    private List<GeoFeature> bagToList(DataBag points) throws ExecException {
        List<GeoFeature> pointList = new ArrayList<GeoFeature>(((Long)points.size()).intValue());
        for (Tuple point : points) {
            if (!point.isNull(0)) {
                String jsonBlob = point.get(0).toString();
                try {
                    GeoFeature poiFeature = (GeoFeature)reader.decode(jsonBlob);
                    pointList.add(poiFeature);
                } catch (JSONException e) {
                    e.printStackTrace(); // gotcha
                }
            } 
        }
        return pointList;
    }

    /**
       Basically just the opposite of bagToList. Takes a list of de-serialized geoJSON features
       and creates a DataBag with the features serialized as geoJSON.
     */
    private DataBag listToBag(List<GeoFeature> features) {
        DataBag bag = bagFactory.newDefaultBag();
        for (GeoFeature feature : features) {
            bag.add(tupleFactory.newTuple(feature.serialize()));
        }
        return bag;
    }

    /**
       Given the geometry representing a tile and a list of points, returns a list containing
       only those points within the tile
     */
    private List<GeoFeature> pointsWithin(Polygon space, List<GeoFeature> points) {
        List<GeoFeature> pointsInside = new ArrayList<GeoFeature>(points.size());
        for (GeoFeature point : points) {
            Point geoPoint = (Point)point.getMfGeometry().getInternalGeometry();
            if (space.contains(geoPoint)) {
                pointsInside.add(point);
            }
        }
        return pointsInside;
    }
    
    /**
       Gets K centers randomly from the passed in list of points, attaches md5ids to them, and returns the
       list of centers.

       FIXME: Should try to get existing centers if they're there
     */
    private List<GeoFeature> getKCenters(String qualifier, String quadkey, List<GeoFeature> points, Integer k) throws ExecException {
        List<GeoFeature> kCenters = new ArrayList<GeoFeature>(points);
        Collections.shuffle(kCenters); // so we can take K centers off the top
        kCenters = kCenters.subList(0, Math.min(k.intValue(), kCenters.size()));
        for (int i = 0; i < kCenters.size(); i++) {
            GeoFeature f = kCenters.get(i);
            String featureId = constructCenterId(qualifier, quadkey, Integer.toString(i));
            GeoFeature center = new GeoFeature(featureId, f.getMfGeometry(), f.getProperties());
            kCenters.set(i, center);
        }
        return kCenters;
    }

    /**
       Uses the passed in qualifier, quadkey, and cluster index to generate a md5id for the cluster.
     */
    private String constructCenterId(String qualifier, String quadkey, String index) {
        String result = null;
        try {
            StringBuffer buffer = new StringBuffer();
            buffer.append(qualifier);
            buffer.append(PERIOD);
            buffer.append(quadkey);
            buffer.append(COLON);
            buffer.append(index);
            result = DigestUtils.md5Hex(buffer.toString().getBytes(CHARSET));
        } catch (Exception e) {
            e.printStackTrace();
        }
        return result;
    }

    /**
       Given a list of centers, as deserialized geoJSON entities (GeoFeatures), generates a
       new HashMap that maps the ids of the centers to a list containing the points
       that are associated with them.
       <p>
       The output will look something like:

       {
         'feauh879hana91ahzj' => ['feauh879hana91ahzj'],
         '890uqnn18afjhanl0z' => ['890uqnn18afjhanl0z'],
         ...
       }

       with the idea that the lists will be added to by the clustering algorithm itself.
       
       FIXME: This HashMap is going to eat a LOT of memory, how can this be ameliorated?
     */
    private HashMap<String, List<GeoFeature>> initNewCenters(List<GeoFeature> currentCenters) {
        HashMap<String, List<GeoFeature>> newCenters = new HashMap<String, List<GeoFeature>>(currentCenters.size());
        for (GeoFeature center : currentCenters) {
            List<GeoFeature> points = new ArrayList<GeoFeature>();
            points.add(center);
            newCenters.put(center.getFeatureId(), points);
        }
        return newCenters;
    }

    /**
       Called after after clustering is done. Only returns centers that have at least
       one point inside the tile in question.
     */
    private List<GeoFeature> filterCentersByInsideTile(List<GeoFeature> centers) {
        List<GeoFeature> result = new ArrayList<GeoFeature>(centers.size());
        for (GeoFeature center : centers) {
            try {
                if (center.getProperties().has(INSIDE_TILE)) {  // {"inside_tile":true} or not there
                    JSONObject centerProperties = new JSONObject(center.getProperties(), JSONObject.getNames(center.getProperties()));
                    centerProperties.remove(INSIDE_TILE);
                    centerProperties.put(TYPE_KEY, CLUSTER_TYPE);
                    GeoFeature finalCenter = new GeoFeature(center.getFeatureId(), center.getMfGeometry(), centerProperties);
                    result.add(finalCenter);
                }
            } catch (JSONException e) {
                e.printStackTrace();
            }
        }
        return result;
    }
    
    /**
       Given a HashMap that maps {center_id => [list_of_points]} will return a new list
       of centers by taking the centroid of all the points for a given center. Ensures that
       the clusters have attached to them information about whether or not they contain
       at least one point inside the tile.

       FIXME: What a fucking mess. Some amount of summarization has to take place here. What is that exactly?
     */
    private List<GeoFeature> computeCentroids(Polygon space, HashMap<String, List<GeoFeature>> centers) {
        
        List<GeoFeature> centroids = new ArrayList<GeoFeature>(centers.size());

        for (Map.Entry<String,List<GeoFeature>> entry : centers.entrySet()) {
            
            List<GeoFeature> points = (List<GeoFeature>)entry.getValue(); // Get the list of points associated with a center

            if (points.size()==1) {
                centroids.add(points.get(0));
            } else {
                List<String> children = new ArrayList<String>(points.size()); // Will hold a center's children ids

                // Add some metadata about the children to the center
                JSONObject metaData = new JSONObject();
            
                // Add all the points associated with a center to the 'CentroidPoint' object
                int numPoints = 0;
                CentroidPoint c = new CentroidPoint();
                for (GeoFeature point : points) {
                    try {
                        // If the child point is a cluster itself, need to accumulate the number of points it has as well
                        boolean isCluster = false;
                        if ((point.getProperties().has(TYPE_KEY) && point.getProperties().getString(TYPE_KEY).equals(CLUSTER_TYPE)) || point.getProperties().has(CLUSTER_KEY)) isCluster = true;
                    
                        if (isCluster) {
                            numPoints += point.getProperties().getInt(CLUSTER_KEY);
                        }
                        // add child id to the children array if it isn't the id of the parent
                        if (!point.getFeatureId().equals(entry.getKey().toString())) children.add(point.getFeatureId()); 
                
                        Point geoPoint = (Point)point.getMfGeometry().getInternalGeometry();

                        if (space.contains(geoPoint)) metaData.put(INSIDE_TILE, true);
                        if (numPoints==0) numPoints = points.size();
                    
                        metaData.put(CLUSTER_KEY, numPoints);
                        c.add(geoPoint);
                    } catch (JSONException e) {
                        e.printStackTrace();
                    }
                }

                try {
                    if (children.size() > 0) metaData.put(CHILDREN_KEY, children); // eg, {"children":['8u9qhjncna90ah', 'jfkah8034ri9z', ...]}
                } catch (JSONException e) {
                    e.printStackTrace();
                }

                // create a new point object from the centroid and add to the centroids list
                Point jtsP = geomFactory.createPoint(c.getCentroid()); 
                MfGeometry mfP = new MfGeometry(jtsP);
                GeoFeature featureP = new GeoFeature(entry.getKey().toString(), mfP, metaData);
                centroids.add(featureP);
            }
        }
        return centroids;
    }
    
    /**
       Given a list of old centers and list of new centers, computes how much different
       the new centers are from the old ones by examining the list of points each center is associated with.
       <p>
       <ul>
       <li>1. Compute jaccard similarity of two clusters with the same id</li>
       <li>2. Take the average of these similarities scores.</li>
       </ul>
       Returns a number between 0 and 1. Closer to 1 means more similar.
     */
    private double similarity(HashMap<String, List<GeoFeature>> oldCenters, HashMap<String, List<GeoFeature>> newCenters) {
        double score = 0.0;
        for (Map.Entry<String, List<GeoFeature>> entry : oldCenters.entrySet()) {
            List<GeoFeature> oldPoints = (List<GeoFeature>)entry.getValue();
            List<GeoFeature> newPoints = (List<GeoFeature>)newCenters.get(entry.getKey().toString());
            score += jaccard(oldPoints, newPoints);
        }
        score /= (double)oldCenters.size();
        return score;
    }

    /**
       Given two lists of geoFeatures computes the jaccard similarity.
     */
    private double jaccard(List<GeoFeature> listA, List<GeoFeature> listB) {
        List<String> listAIds = new ArrayList<String>(listA.size()); // :)
        List<String> listBIds = new ArrayList<String>(listB.size());

        for (GeoFeature f : listA) {
            if (f.getFeatureId()!=null) listAIds.add(f.getFeatureId());
        }
        for (GeoFeature f : listB) {
            if (f.getFeatureId()!=null) listBIds.add(f.getFeatureId());
        }
        
        if (listAIds == listBIds) return 1.0;
        Collections.sort(listAIds);
        Collections.sort(listBIds);
        if (listAIds == listBIds) return 1.0;

        int listASize = listAIds.size();
        int listBSize = listBIds.size(); 
        int togetherSize = listASize + listBSize;
        int differenceSize = 0;
        int intersectSize = 0;
        int unionSize = togetherSize;

        if (listASize > listBSize) {
            listAIds.removeAll(listBIds);
            differenceSize = listAIds.size();
            intersectSize = listASize - differenceSize;
        } else {
            listBIds.removeAll(listAIds);
            differenceSize = listBIds.size();
            intersectSize = listBSize - differenceSize;
        }
        if (intersectSize==0) return 0.0;
        unionSize = togetherSize - intersectSize;
        double result = ((double)intersectSize)/((double)unionSize);
        return result;
    }
}
