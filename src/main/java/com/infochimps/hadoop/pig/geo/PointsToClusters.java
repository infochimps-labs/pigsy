package com.infochimps.hadoop.pig.geo;

import java.io.IOException;
import java.util.List;
import java.util.ArrayList;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.backend.executionengine.ExecException;

import org.mapfish.geo.MfGeo;
import org.mapfish.geo.MfGeoFactory;
import org.mapfish.geo.MfGeoJSONReader;
import org.mapfish.geo.MfGeoJSONWriter;
import org.mapfish.geo.MfGeometry;
import org.mapfish.geo.MfFeature;

import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.geom.Polygon;

import org.json.JSONStringer;
import org.json.JSONException;
import org.json.JSONObject;


/**
   Takes two data bags of input. The first is a bag of points, the second is a bag
   of clusters represented as polygons (eg. the spatial extent of a cluster).
   <p>
   The first step is to attach to every point the set of clusters it lies within.
   Returns a bag of tuples like the following:
   (cluster_id, cluster_domain_id, point_as_geojson_point)
   This way we can then GROUP BY cluster_id to generate an entire cluster. This cluster
   is then dispatched to the set of tiles it maps to and stored into HBase.
   <p>
   Note that, after running this udf on all the input points, you MAY have less points
   than when you started. This will only happen when the input points map to no clusters
 */
public final class PointsToClusters extends EvalFunc<DataBag> {

    private static TupleFactory tupleFactory = TupleFactory.getInstance();
    private static BagFactory   bagFactory = BagFactory.getInstance();
    
    private final MfGeoFactory mfFactory = new MfGeoFactory() {
            public MfFeature createFeature(String id, MfGeometry geometry, JSONObject properties) {
                return new GeoFeature(id, geometry, properties);
            }
        };
    
    private final MfGeoJSONReader reader = new MfGeoJSONReader(mfFactory);

    public DataBag exec(Tuple input) throws IOException {
        if (input == null || input.size() < 2 || input.isNull(0) || input.isNull(1))
            return null;
        DataBag pointBag = (DataBag)input.get(0);   // points
        DataBag clusterBag = (DataBag)input.get(1); // clusters, as polygons
        if (clusterBag.size()==0) {
            // This is the case in which the clusters do not entirely fill
            // the space. We want to know about it in the logs but DO NOT FAIL.
            log.warn("No clusters found");
            return null;
        };
        DataBag returnBag = bagFactory.newDefaultBag();

        List<GeoFeature> clusters = bagToList(clusterBag);
        for (Tuple pointTuple : pointBag) {
            try {
                String pointJson = pointTuple.get(0).toString();
                GeoFeature pointFeature = (GeoFeature)reader.decode(pointJson);
                for (GeoFeature cluster : clusters) {
                    Geometry clusterExtent = cluster.getMfGeometry().getInternalGeometry();
                    if (clusterExtent.contains(pointFeature.getMfGeometry().getInternalGeometry())) {
                        Tuple t = tupleFactory.newTuple(3);
                        t.set(0, cluster.getFeatureId());
                        t.set(1, cluster.getProperties().get("_domain_id"));
                        t.set(2, pointJson);
                        returnBag.add(t);
                    }

                }
            } catch (JSONException e) {
                e.printStackTrace();
            }
        }
        
        return returnBag;
    }

    private List<GeoFeature> bagToList(DataBag features) throws ExecException {
        List<GeoFeature> featureList = new ArrayList<GeoFeature>(((Long)features.size()).intValue());
        for (Tuple feature : features) {
            if (!feature.isNull(0)) {
                String jsonBlob = feature.get(0).toString();
                try {
                    GeoFeature geoFeature = (GeoFeature)reader.decode(jsonBlob);
                    featureList.add(geoFeature);
                } catch (JSONException e) {
                    e.printStackTrace(); // gotcha
                }
            } 
        }
        return featureList;
    }
}
