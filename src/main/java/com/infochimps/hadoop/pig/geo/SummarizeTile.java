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
   
   Given a tile quadkey and a bag of points, runs K-means
   clustering over the points. Note that if the DispatchPoints
   UDF was used (and it _should_ be used here) then the bag of
   points will contain not only the points for the tile specified
   by the quadkey but also those of its neighbors. This is to
   capture clusters near the edges of tiles properly.
   
 */
public class SummarizeTile extends EvalFunc<HashMap<String,DataBag>> {

    private static TupleFactory tupleFactory = TupleFactory.getInstance();
    private static BagFactory   bagFactory = BagFactory.getInstance();
    
    private static final Long MAX_POINTS_PER_TILE = 75l;
    private static final String CLUSTERS = "clusters";
    private static final String POINTS = "points";
    
    // Simple factory for creating geoJSON features from json strings
    private final MfGeoFactory mfFactory = new MfGeoFactory() {
            public MfFeature createFeature(String id, MfGeometry geometry, JSONObject properties) {
                return new GeoFeature(id, geometry, properties);
            }
        };

    private static final GeometryFactory geomFactory = new GeometryFactory();
    private final MfGeoJSONReader reader = new MfGeoJSONReader(mfFactory);
    
    public HashMap exec(Tuple input) throws IOException {
        if (input == null || input.size() < 2)
            return null;

        HashMap<String,DataBag> bags = new HashMap<String,DataBag>();
        String quadKey = input.get(0).toString();
        DataBag points = (DataBag)input.get(1);
        if (points.size() < MAX_POINTS_PER_TILE) { // if there aren't enough points, don't cluster
            bags.put(POINTS, points);
            return bags;
        } else {
        
            DataBag clusters = bagFactory.newDefaultBag(); // will contain clusters if any
            Polygon space = QuadKeyUtils.quadKeyToBox(quadKey);
            
            List<GeoFeature> pointList = bagToList(points);
            List<GeoFeature> kCenters = getKCenters(space, pointList, 25);
            for (GeoFeature center : kCenters) { // cull centers from points to compare to
                pointList.remove(center);
            }

            GeoFeature firstCenter = kCenters.get(0);
            String firstCenterId = firstCenter.getFeatureId();
            Point firstCenterPoint = (Point)firstCenter.getMfGeometry().getInternalGeometry();

            HashMap<String, List<GeoFeature>> newCenters = initNewCenters(kCenters);
            
            for (GeoFeature point : pointList) {
                Point geoPoint = (Point)point.getMfGeometry().getInternalGeometry();
                double distance = geoPoint.distance(firstCenterPoint);
                String centerId = firstCenterId;

                // find nearest center
                for (GeoFeature center : kCenters) {
                    Point centerPoint = (Point)center.getMfGeometry().getInternalGeometry();
                    double distanceToCenter = geoPoint.distance(centerPoint);
                    if (distanceToCenter < distance) {
                        distance = distanceToCenter;
                        centerId = center.getFeatureId();
                    }   
                }
                List<GeoFeature> nearestCentersPoints = (List<GeoFeature>)newCenters.get(centerId);
                nearestCentersPoints.add(point);
                newCenters.put(centerId, nearestCentersPoints); 
            }
            
            bags.put(CLUSTERS, clusters);
            return bags;
        }
    }

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
    
    // You'd better give me a bag of points that has _at least_ k points in it
    private List<GeoFeature> getKCenters(Polygon space, List<GeoFeature> points, Integer k) throws ExecException {
        List<GeoFeature> kCenters = new ArrayList<GeoFeature>();
        for (GeoFeature point : points) {
            Point geoPoint = (Point)point.getMfGeometry().getInternalGeometry();
            if (space.contains(geoPoint)) {
                kCenters.add(point);
            }
        }
        Collections.shuffle(kCenters);
        kCenters = kCenters.subList(0, Math.min(k.intValue(), kCenters.size()));
        return kCenters;
    }

    private HashMap<String, List<GeoFeature>> initNewCenters(List<GeoFeature> currentCenters) {
        HashMap<String, List<GeoFeature>> newCenters = new HashMap<String, List<GeoFeature>>();
        for (GeoFeature center : currentCenters) {
            List<GeoFeature> points = new ArrayList<GeoFeature>();
            points.add(center);
            newCenters.put(center.getFeatureId(), points);
        }
        return newCenters;
    }

    private HashMap<String, Point> computeCentroids(HashMap<String, List<GeoFeature>> centers) {
        HashMap<String, Point> centroids = new HashMap<String, Point>();
        for (Map.Entry<String,List<GeoFeature>> entry : centers.entrySet()) {
            List<GeoFeature> points = (List<GeoFeature>)entry.getValue();
            CentroidPoint c = new CentroidPoint();
            for (GeoFeature point : points) {
                Point geoPoint = (Point)point.getMfGeometry().getInternalGeometry();
                c.add(geoPoint);
            }
            Point p = geomFactory.createPoint(c.getCentroid());
            centroids.put(entry.getKey().toString(), p);
        }
        return centroids;
    }
    
    /**
       Needs to create new GeoFeatures by summarizing the ones attached to each center
     */
    private List<GeoFeature> summarizeCenters(HashMap<String, List<GeoFeature>> centers) {
        List<GeoFeature> summary = new ArrayList<GeoFeature>();
        for (Map.Entry<String,List<GeoFeature>> entry : centers.entrySet()) {
            
        }
        return summary;
    }
}
