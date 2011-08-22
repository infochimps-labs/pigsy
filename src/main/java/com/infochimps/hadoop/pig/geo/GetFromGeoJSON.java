package com.infochimps.hadoop.pig.geo;

import java.io.IOException;
import java.util.HashMap;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.TupleFactory;

import org.mapfish.geo.MfGeo;
import org.mapfish.geo.MfGeoFactory;
import org.mapfish.geo.MfGeoJSONReader;
import org.mapfish.geo.MfGeometry;
import org.mapfish.geo.MfFeature;

import com.vividsolutions.jts.geom.Polygon;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.Point;

import org.json.JSONException;
import org.json.JSONObject;

/**
   Given a geoJSON feature, returns the field specified. Eg. 'properties._type' will return the '_type' field
   from the 'properties' hash of the input geoJSON.
 */
public class GetFromGeoJSON extends EvalFunc<String> {
    private final MfGeoFactory mfFactory = new MfGeoFactory() {
            public MfFeature createFeature(String id, MfGeometry geometry, JSONObject properties) {
                return new GeoFeature(id, geometry, properties);
            }
        };

    private final MfGeoJSONReader reader = new MfGeoJSONReader(mfFactory);

    private static final String PERIOD = "\\.";
    private static final String GEOJSON_ID = "id";
    private static final String GEOJSON_TYPE = "type";
    private static final String GEOJSON_FEATURE = "Feature";
    private static final String GEOJSON_GEOM = "geometry";
    private static final String GEOJSON_PROP = "properties";
    
    public String exec(Tuple input) throws IOException {
        if (input == null || input.size() < 2 || input.isNull(0) || input.isNull(1))
            return null;

        String fieldString = input.get(0).toString();
        String json = input.get(1).toString();
        String result = null;
        try {
            GeoFeature feature = (GeoFeature)reader.decode(json);
            if (fieldString.equals(GEOJSON_ID)) {
                result = feature.getFeatureId();
            } else if (fieldString.equals(GEOJSON_TYPE)) {
                result = GEOJSON_FEATURE;
            } else if (fieldString.startsWith(GEOJSON_GEOM)) {
                if (fieldString.endsWith(GEOJSON_TYPE)) {
                    result = feature.getMfGeometry().getInternalGeometry().getGeometryType();
                } else {
                    result = feature.getMfGeometry().getInternalGeometry().toText();
                }                
            } else if (fieldString.startsWith(GEOJSON_PROP)){
                String[] nestedFields = fieldString.split(PERIOD);
                if (nestedFields.length > 1) {
                    Object value = feature.getFromProperties(nestedFields[1]);
                    for (int i = 2; i < nestedFields.length; i++) {
                        value = ((HashMap<String, Object>)value).get(nestedFields[i]);
                    }
                    result = (value != null ? value.toString() : null);
                }                
            } 

        } catch (JSONException e) {
            e.printStackTrace();
        }
        return result;
    }
}
