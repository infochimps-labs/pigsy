package com.infochimps.hadoop.pig.geo;

import java.io.IOException;
import java.util.List;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.HashMap; 

import org.apache.commons.codec.digest.DigestUtils;

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

import com.vividsolutions.jts.geom.*;
import com.vividsolutions.jts.algorithm.CentroidPoint;

import org.json.JSONStringer;
import org.json.JSONException;
import org.json.JSONObject;

/**
   Given a cluster represented as a polygon and the points that fall inside it,
   generates a new cluster.
   <p>
   The _domain_id of the cluster implies a set of zoom levels. 
   <p>
   1. Summarize the points (take centroid, count points) and generate 'Point' geojson features
   2. Using the namespace, protocol, _type and the _domain_id, generate an md5id for the cluster
   3. Generate the set of tiles the cluster will map to (uses the _domain_id)
   4. Yields {(quadkey, cluster_json),...}
   
 */
public final class CreateClusters extends EvalFunc<DataBag> {

    private static TupleFactory tupleFactory = TupleFactory.getInstance();
    private static BagFactory   bagFactory = BagFactory.getInstance();
    
    private final MfGeoFactory mfFactory = new MfGeoFactory() {
            public MfFeature createFeature(String id, MfGeometry geometry, JSONObject properties) {
                return new GeoFeature(id, geometry, properties);
            }
        };

    private static final GeometryFactory geomFactory = new GeometryFactory();
    private final MfGeoJSONReader reader = new MfGeoJSONReader(mfFactory);

    private final String COLON = ":";
    private final String CHARSET = "UTF-8";
    
    // The keys to use in the geoJSON serialization of a cluster. Indicates the number of points used.
    private static final String CLUSTER_KEY = "_total";
    private static final String CHILDREN_KEY = "children";
    private static final String TYPE_KEY = "_type";
    private static final String CLUSTER_TYPE = "cluster_point";
    private static final String UNDERSCORE = "_";

    private static final HashMap<Integer, List<Integer>> zoomLevelMap = new HashMap<Integer, List<Integer>>() {
        {
            put(1, Arrays.asList(1,2,3,4,5));
            put(2, Arrays.asList(6,7,8,9,10));
            put(3, Arrays.asList(11,12,13,14,15)); // the max here is the max zoom level at which clusters exist
        }
    };
    
    public DataBag exec(Tuple input) throws IOException {
        if (input == null || input.size() < 3 || input.isNull(0) || input.isNull(1) || input.isNull(2))
            return null;
        String qualifier = input.get(0).toString(); // namespace.protocol.type
        String domainId = input.get(1).toString();  // cluster domain id
        DataBag pointBag = (DataBag)input.get(2);   // geojson points for the cluster
        DataBag returnBag = bagFactory.newDefaultBag();

        GeoFeature resultCluster = summarizePoints(qualifier, domainId, pointBag);
        Point resultCenter = (Point)resultCluster.getMfGeometry().getInternalGeometry();
        String result = resultCluster.serialize();
        List<Integer> zoomLevels = getZoomLevels(domainId);
        for (Integer zoomLevel : zoomLevels) {
            String quadKey = QuadKeyUtils.geoPointToQuadKey(resultCenter.getX(), resultCenter.getY(), zoomLevel);
            Tuple returnTuple = tupleFactory.newTuple(2);
            returnTuple.set(0, quadKey);
            returnTuple.set(1, result);
            returnBag.add(returnTuple);
        }
        return returnBag;
    }

    private GeoFeature summarizePoints(String qualifier, String domainId, DataBag pointBag) throws ExecException {
        List<String> children = new ArrayList<String>(((Long)pointBag.size()).intValue()); // Will hold a center's children ids
        JSONObject metaData = new JSONObject();
        CentroidPoint c = new CentroidPoint();
        for (Tuple pointTuple : pointBag) {
            try {
                String pointJson = pointTuple.get(0).toString();
                GeoFeature pointFeature = (GeoFeature)reader.decode(pointJson);
                children.add(pointFeature.getFeatureId());

                Point geoPoint = (Point)pointFeature.getMfGeometry().getInternalGeometry();
                c.add(geoPoint);
            } catch (JSONException e) {
                e.printStackTrace();
            }
        }

        try {
            metaData.put(CLUSTER_KEY, pointBag.size());
            metaData.put(TYPE_KEY, CLUSTER_TYPE);
            if (children.size() > 0) metaData.put(CHILDREN_KEY, children); // eg, {"children":['8u9qhjncna90ah', 'jfkah8034ri9z', ...]}
        } catch (JSONException e) {
            e.printStackTrace();
        }
        
        Point centroid = geomFactory.createPoint(c.getCentroid());
        MfGeometry mfCentroid = new MfGeometry(centroid);
        GeoFeature result = new GeoFeature(constructCenterId(qualifier, domainId), mfCentroid, metaData);
        return result;
    }

    /**
       Uses the passed in qualifier, quadkey, and cluster index to generate a md5id for the cluster.
    */
    private String constructCenterId(String qualifier, String domainId) {
        String result = null;
        try {
            StringBuffer buffer = new StringBuffer();
            buffer.append(qualifier);
            buffer.append(COLON);
            buffer.append(domainId);
            result = DigestUtils.md5Hex(buffer.toString().getBytes(CHARSET));
        } catch (Exception e) {
            e.printStackTrace();
        }
        return result;
    }

    private List<Integer> getZoomLevels(String domainId) {
        String[] splitId = domainId.split(UNDERSCORE);
        List<Integer> levels = (List<Integer>)zoomLevelMap.get(splitId.length-1);
        return levels;
    }
}
