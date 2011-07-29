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
   Attaches data to geoJSON. Only allows to set the 'id' of the geoJSON and !!!_non-nested_!!!
   fields in the 'properties' hash. All other fields are ignored.
 */
public class AttachToGeoJSON extends EvalFunc<String> {
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
        if (input == null || input.size() < 3 || input.isNull(0) || input.isNull(1))
            return null;

        String fieldString = input.get(0).toString();
        String data = input.get(1).toString();
        String json = input.get(2).toString();
        String result = json;
        GeoFeature resultFeature = null;
        try {
            GeoFeature feature = (GeoFeature)reader.decode(json);
            if (fieldString.equals(GEOJSON_ID)) {
                resultFeature = new GeoFeature(data, feature.getMfGeometry(), feature.getProperties());
                result = resultFeature.serialize();
            } else if (fieldString.startsWith(GEOJSON_PROP)){
                String[] nestedFields = fieldString.split(PERIOD);
                if (nestedFields.length > 1) {
                    resultFeature = new GeoFeature(feature.getFeatureId(), feature.getMfGeometry(), feature.getProperties().put(nestedFields[1], data));
                    result = resultFeature.serialize();
                }
            } 
        } catch (JSONException e) {}
        return result;
    }
}
