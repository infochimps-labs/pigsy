package com.infochimps.hadoop.pig.geo;

import java.io.IOException;

import org.apache.commons.codec.digest.DigestUtils;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.tools.pigstats.PigStatusReporter;

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
   Inspects the field specified of input geoJSON and determines whether it
   exists and whether or not the value is null. Returns the record itself.
 */
public class ValidateGeoJSON extends EvalFunc<String> {
    private final MfGeoFactory mfFactory = new MfGeoFactory() {
            public MfFeature createFeature(String id, MfGeometry geometry, JSONObject properties) {
                return new GeoFeature(id, geometry, properties);
            }
        };

    static enum GeoJSON { NULL_FIELD, MISSING_FIELD, WITH_COLONS, WITH_SPACES, WITH_SLASHES };
    
    private final MfGeoJSONReader reader = new MfGeoJSONReader(mfFactory);

    private static final String PERIOD = "\\.";
    private static final String GEOJSON_ID = "id";
    private static final String GEOJSON_TYPE = "type";
    private static final String GEOJSON_FEATURE = "Feature";
    private static final String GEOJSON_GEOM = "geometry";
    private static final String GEOJSON_PROP = "properties";
    private static final String COLON = ":";
    private static final String SLASH = "/";
    private static final String SPACE = " ";
    
    public String exec(Tuple input) throws IOException {
        if (input == null || input.size() < 2 || input.isNull(0) || input.isNull(1))
            return null;

        String fieldString = input.get(0).toString();
        String json = input.get(1).toString();
        Object toCheck = null;

        PigStatusReporter reporter = PigStatusReporter.getInstance();
                    
        try {
            GeoFeature feature = (GeoFeature)reader.decode(json);
            if (fieldString.equals(GEOJSON_ID)) {
                toCheck = feature.getFeatureId();                
            } else if (fieldString.startsWith(GEOJSON_PROP)){
                String[] nestedFields = fieldString.split(PERIOD);
                if (nestedFields.length > 1) {
                    if (feature.getProperties().has(nestedFields[1])) {
                        toCheck = feature.getFromProperties(nestedFields[1]);
                        for (int i = 2; i < nestedFields.length; i++) {
                            if (((JSONObject)toCheck).has(nestedFields[i])) {
                                toCheck = ((JSONObject)toCheck).get(nestedFields[i]);
                            }
                        }
                    } else {
                        if (reporter != null) {
                            reporter.getCounter(GeoJSON.MISSING_FIELD).increment(1); 
                        }
                    }
                }
            } 
        } catch (JSONException e) {
            e.printStackTrace();
        }

        // Why would it be null you ask? Pig runs this udf couple of times before the mapreduce job is actually launched
        if (reporter != null) {
            if (toCheck==null) {
                reporter.getCounter(GeoJSON.NULL_FIELD).increment(1);
            } else if (toCheck.toString().contains(COLON)) {
                reporter.getCounter(GeoJSON.WITH_COLONS).increment(1);
            } else if (toCheck.toString().contains(SLASH)) {
                reporter.getCounter(GeoJSON.WITH_SLASHES).increment(1);
            } else if (toCheck.toString().contains(SPACE)) {
                reporter.getCounter(GeoJSON.WITH_SPACES).increment(1);
            }
        }
        return json;
    }
}
