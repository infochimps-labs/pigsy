package com.infochimps.hadoop.pig.geo;

import java.io.IOException;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import java.util.Collections;

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
public class SummarizeTile extends EvalFunc<HashMap<String,DataBag>> {

    private static TupleFactory tupleFactory = TupleFactory.getInstance();
    private static BagFactory   bagFactory = BagFactory.getInstance();

    // The maximum number of points to allow a tile to have before clusters are generated
    private static final Long MAX_POINTS_PER_TILE = 75l;
    
    private static final String CLUSTERS = "clusters";
    private static final String POINTS = "points";

    // The keys to use in the geoJSON serialization of a cluster. Indicates the number of points used.
    private static final String CLUSTER_KEY = "num_points";
    private static final String INSIDE_TILE = "inside_tile";
    
    // Simple factory for creating geoJSON features from json strings
    private final MfGeoFactory mfFactory = new MfGeoFactory() {
            public MfFeature createFeature(String id, MfGeometry geometry, JSONObject properties) {
                return new GeoFeature(id, geometry, properties);
            }
        };

    private static final GeometryFactory geomFactory = new GeometryFactory();
    private final MfGeoJSONReader reader = new MfGeoJSONReader(mfFactory);
    
    public HashMap exec(Tuple input) throws IOException {
        if (input == null || input.size() < 3)
            return null;

        HashMap<String,DataBag> bags = new HashMap<String,DataBag>(); // Result

        // Get arguments from the input tuple
        Integer numCenters = (Integer)input.get(0);
        String quadKey = input.get(1).toString();
        DataBag points = (DataBag)input.get(2);

        Polygon space = QuadKeyUtils.quadKeyToBox(quadKey); // Get the tile as a geometry object
        
        if (points.size() < MAX_POINTS_PER_TILE) { // if there aren't enough points, don't cluster
            // FIXME: Need to return only points that are actually inside the tile.
            List<GeoFeature> pointsDeserialized = bagToList(points);
            List<GeoFeature> inside = pointsWithin(space, pointsDeserialized);
            bags.put(POINTS, listToBag(inside));
            return bags;
        } else {
        
            
            
            List<GeoFeature> pointList = bagToList(points);
            List<GeoFeature> kCenters = getKCenters(space, pointList, numCenters);

            //
            // Whoops, still not enough points for clustering
            // FIXME: Make sure we only return points inside the tile
            //
            if (kCenters.size() < numCenters) {
                List<GeoFeature> pointsDeserialized = bagToList(points);
                List<GeoFeature> inside = pointsWithin(space, pointsDeserialized);
                bags.put(POINTS, listToBag(inside));
                return bags;
            }
            //
            
            for (GeoFeature center : kCenters) {
                // DO NOT include the centers in the calculation
                pointList.remove(center);
            }

            // Create a HashMap that maps {center_id => [list of points]}
            HashMap<String, List<GeoFeature>> currentCenters = initNewCenters(kCenters);
            
            //
            // Iterate _at most_ 100 times for k-means
            //
            for (int i = 0; i < 100; i++) {

                // Get information about the first center as a starting point for the calculation
                GeoFeature firstCenter = kCenters.get(0);
                String firstCenterId = firstCenter.getFeatureId();
                Point firstCenterPoint = (Point)firstCenter.getMfGeometry().getInternalGeometry();

                HashMap<String, List<GeoFeature>> newCenters = initNewCenters(kCenters);
                
                for (GeoFeature point : pointList) {
                    Point geoPoint = (Point)point.getMfGeometry().getInternalGeometry();
                    double distance = geoPoint.distance(firstCenterPoint);
                    String centerId = firstCenterId;

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

                //
                // Now, we have a giant hashmap of centers, need to calculate their centroids and
                // create a new list of centers. This list of centers must then be compared to the
                // existing list of centers to determine whether or not convergence has happened.
                //

                // Break if the new centers are the same as the old centers
                if (similarity(currentCenters, newCenters) >= 0.99) break;

                // Update K centers to be centroids
                kCenters = computeCentroids(space, newCenters);

                // copy new centers to current centers
                currentCenters = (HashMap<String, List<GeoFeature>>)newCenters.clone();
                currentCenters.putAll(newCenters);
            }

            // FIXME: Only return clusters that have at least one point inside the tile
            List<GeoFeature> finalCenters = filterCentersByInsideTile(kCenters);
            bags.put(CLUSTERS, listToBag(finalCenters));
            return bags;
        }
    }

    /**
       Given a Pig DataBag which contains tuples with exactly one element, namely the de-serialized
       geoJSON point entities, will return a List containing these same entities.
     */
    private List<GeoFeature> bagToList(DataBag points) throws ExecException {
        List<GeoFeature> pointList = new ArrayList<GeoFeature>(((Long)points.size()).intValue());
        for (Tuple point : points) {
            if (!point.isNull(0)) {
                String jsonBlob = point.get(0).toString();
                try {
                    GeoFeature poiFeature = (GeoFeature)reader.decode(jsonBlob);
                    pointList.add(poiFeature);
                } catch (JSONException e) {}
            } 
        }
        return pointList;
    }

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
        List<GeoFeature> pointsInside = new ArrayList<GeoFeature>();
        for (GeoFeature point : points) {
            Point geoPoint = (Point)point.getMfGeometry().getInternalGeometry();
            if (space.contains(geoPoint)) {
                pointsInside.add(point);
            }
        }
        return pointsInside;
    }
    
    /**
       Given the geometry representing a tile, a list of points, and some number of centers to return (K),
       this method returns K points randomly selected from the subset of points that are inside the space.
     */
    private List<GeoFeature> getKCenters(Polygon space, List<GeoFeature> points, Integer k) throws ExecException {
        List<GeoFeature> kCenters = new ArrayList<GeoFeature>(points);
        Collections.shuffle(kCenters);
        kCenters = kCenters.subList(0, Math.min(k.intValue(), kCenters.size()));
        return kCenters;
    }

    /**
       Given a list of centers, as deserialized geoJSON entities (GeoFeatures), generates a
       new HashMap that maps the ids of the centers to a list containing the GeoFeatures
       that belong to them.
     */
    private HashMap<String, List<GeoFeature>> initNewCenters(List<GeoFeature> currentCenters) {
        HashMap<String, List<GeoFeature>> newCenters = new HashMap<String, List<GeoFeature>>();
        for (GeoFeature center : currentCenters) {
            List<GeoFeature> points = new ArrayList<GeoFeature>();
            points.add(center);
            newCenters.put(center.getFeatureId(), points);
        }
        return newCenters;
    }

    private List<GeoFeature> filterCentersByInsideTile(List<GeoFeature> centers) {
        List<GeoFeature> result = new ArrayList<GeoFeature>();
        for (GeoFeature center : centers) {
            try {
                if (center.getProperties().get(INSIDE_TILE) != null) {
                    result.add(center);
                }
            } catch (JSONException e) {/* whoops */};
        }
        return result;
    }
    
    /**
       Given a HashMap that maps {center_id => [list_of_points]} will return a new list
       of centers by taking the centroid of all the points for a given center.

       FIXME: Some amount of summarization has to take place here. What is that exactly?
     */
    private List<GeoFeature> computeCentroids(Polygon space, HashMap<String, List<GeoFeature>> centers) {
        List<GeoFeature> centroids = new ArrayList<GeoFeature>(centers.size());
        for (Map.Entry<String,List<GeoFeature>> entry : centers.entrySet()) {
            List<GeoFeature> points = (List<GeoFeature>)entry.getValue();
            JSONObject metaData = new JSONObject();
            CentroidPoint c = new CentroidPoint();
            for (GeoFeature point : points) {
                Point geoPoint = (Point)point.getMfGeometry().getInternalGeometry();
                try {
                    if (space.contains(geoPoint)) metaData.put(INSIDE_TILE, true);
                } catch (JSONException e) {/* whoops */}
                c.add(geoPoint);
            }
            Point jtsP = geomFactory.createPoint(c.getCentroid());
            MfGeometry mfP = new MfGeometry(jtsP);
            try {
                metaData.put(CLUSTER_KEY, points.size());
            } catch (JSONException e) {/* whoops */}
            GeoFeature featureP = new GeoFeature(entry.getKey().toString(), mfP, metaData);
            centroids.add(featureP);
        }
        return centroids;
    }

    /**
       Given a List of GeoFeature objects, returns a HashMap containing {GeoFeature.id => GeoFeature}
       for convenience.
     */
    private HashMap<String, GeoFeature> listToMap(List<GeoFeature> features) {
        HashMap<String, GeoFeature> result = new HashMap<String, GeoFeature>(features.size());
        for (GeoFeature feature : features) {
            result.put(feature.getFeatureId(), feature);
        }
        return result;
    }
    
    /**
       Given a list of old centers and list of new centers, computes how much different
       the new centers are from the old ones.
       <p>
       <ul>
       <li>1. Compute jaccard similarity of two clusters with the same id</li>
       <li>2. Take the average of these similarities scores.</li>
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
        for (GeoFeature f : listA) listAIds.add(f.getFeatureId());
        for (GeoFeature f : listB) listBIds.add(f.getFeatureId());
        
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
