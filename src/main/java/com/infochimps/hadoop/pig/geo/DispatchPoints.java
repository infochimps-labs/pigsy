package com.infochimps.hadoop.pig.geo;

import java.io.IOException;
import java.util.List;

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

import com.vividsolutions.jts.geom.Polygon;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.Point;

import org.json.JSONException;
import org.json.JSONObject;

public class DispatchPoints extends EvalFunc<DataBag> {
    private static TupleFactory tupleFactory = TupleFactory.getInstance();
    private static BagFactory   bagFactory = BagFactory.getInstance();
    private static final String GEOM_POINT = "Point";

    // Simple factory for creating geoJSON features from json strings
    private final MfGeoFactory mfFactory = new MfGeoFactory() {
            public MfFeature createFeature(String id, MfGeometry geometry, JSONObject properties) {
                return new GeoFeature(id, geometry, properties);
            }
        };

    private final MfGeoJSONReader reader = new MfGeoJSONReader(mfFactory);

    public DataBag exec(Tuple input) throws IOException {
        if (input == null || input.size() < 2)
            return null;

        DataBag returnKeys = bagFactory.newDefaultBag();
        Integer resolution = (Integer)input.get(0);
        String jsonBlob = input.get(1).toString();
        try {
            MfGeo result = reader.decode(jsonBlob);
            GeoFeature feature = (GeoFeature)result;
            MfGeometry geom = feature.getMfGeometry();
            Geometry jts = geom.getInternalGeometry();

            if (jts.getGeometryType().equals(GEOM_POINT)) {
                // There had better be 9 of them
                List<String> keyAndNeighbors = QuadKeyUtils.quadKeyAndNeighbors(((Point)jts).getX(), ((Point)jts).getY(), resolution);
                for (String quadKey : keyAndNeighbors) {
                    System.out.println();
                    Tuple newQuadKey = tupleFactory.newTuple(quadKey);
                    returnKeys.add(newQuadKey);
                }
            } else {/* I regret to inform you, but this only works with points. */}
        } catch (JSONException e) {}
        // DEBUG
        System.out.println(QuadKeyUtils.serializeBagOfQuadkeys(returnKeys));
        //
        return returnKeys;
    }
}
