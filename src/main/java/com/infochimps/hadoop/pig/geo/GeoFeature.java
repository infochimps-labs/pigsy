package com.infochimps.hadoop.pig.geo;

import java.util.Iterator;

import org.mapfish.geo.MfGeoJSONWriter;
import org.mapfish.geo.MfGeometry;
import org.mapfish.geo.MfFeature;

import org.json.JSONStringer;
import org.json.JSONObject;
import org.json.JSONWriter;
import org.json.JSONArray;
import org.json.JSONException;

public class GeoFeature extends MfFeature {
    private final String id;
    private final MfGeometry geometry;
    private final JSONObject properties;

    public GeoFeature(String id, MfGeometry geometry, JSONObject properties) {
        this.id = id;
        this.geometry = geometry;
        this.properties = properties;
    }

    public String serialize() {
        JSONStringer stringer   = new JSONStringer();
        MfGeoJSONWriter builder = new MfGeoJSONWriter(stringer);
        try {
            builder.encodeFeature(this);
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
        return stringer.toString();
    }


    public String getFeatureId() {
        return id;
    }

    public JSONObject getProperties() {
        return properties;
    }

    public Object getFromProperties(String key) {
        Object result = null;
        try {
            result = properties.get(key);
        } catch (JSONException e) {}
        return result;
    }

    public MfGeometry getMfGeometry() {
        return geometry;
    }

    public void toJSON(JSONWriter builder) throws JSONException {
        createJSON(builder, properties);
    }

    //
    // Fuck you java. Seriously, fuck you. One would assume the JSON library
    // would already have this method. However, that would only be sane. If you're
    // looking for sanity then you're in the wrong place buddy.
    //
    public void createJSON(JSONWriter builder, Object o) throws JSONException {
            
        if (o instanceof JSONObject) {
            Iterator keyItr = ((JSONObject)o).keys();
            while (keyItr.hasNext()) {
                String key   = keyItr.next().toString();
                Object value = ((JSONObject)o).get(key);
                builder.key(key);
                if (value instanceof JSONObject) {
                    builder.object();
                    createJSON(builder, value);
                    builder.endObject();
                } else if (value instanceof JSONArray) {
                    builder.array();
                    createJSON(builder, value);
                    builder.endArray();
                } else {
                    builder.value(value);
                }
                
            }
        } else if (o instanceof JSONArray) {
            for (int i = 0; i < ((JSONArray)o).length(); i++) {
                Object value = ((JSONArray)o).get(i);
                if (value instanceof JSONObject) {
                    builder.object();
                    createJSON(builder, value);
                    builder.endObject();
                } else if (value instanceof JSONArray) {
                    builder.array();
                    createJSON(builder, value);
                    builder.endArray();
                } else {
                    builder.value(value);
                }
            }
        }
    } 
}
