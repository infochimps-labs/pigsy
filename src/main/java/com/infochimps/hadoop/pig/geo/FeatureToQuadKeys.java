package com.infochimps.hadoop.pig.geo;

import java.io.IOException;

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

import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.Point;

import org.json.JSONException;
import org.json.JSONObject;

public class FeatureToQuadKeys extends EvalFunc<DataBag> {

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

        DataBag returnCells = bagFactory.newDefaultBag();
        Integer resolution  = (Integer)input.get(0);
        Object jsonObj      = input.get(1);

        // Return an empty bag if no json string is supplied
        if (jsonObj == null) {
            return returnCells;
        }
        
        String jsonBlob       = jsonObj.toString();
        try {
            // All the gunk involved with reading in a 
            MfGeo result       = reader.decode(jsonBlob);
            GeoFeature feature = (GeoFeature)result;
            MfGeometry geom    = feature.getMfGeometry();
            Geometry jts       = geom.getInternalGeometry();

            // If the object is a simple point then act on it immediately and without further adeau
            if (jts.getGeometryType().equals(GEOM_POINT)) {
                Point point   = (Point) jts;
                Tuple geocell = tupleFactory.newTuple(QuadKeyUtils.geoPointToQuadKey(point.getX(), point.getY(), resolution));
                returnCells.add(geocell);
            } else {
                returnCells = QuadKeyUtils.allCellsFor(jts, resolution);
            }
        } catch (JSONException e) {}
        return returnCells;
    }
}
