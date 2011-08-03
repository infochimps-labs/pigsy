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
import com.vividsolutions.jts.geom.Coordinate;

import org.json.JSONException;
import org.json.JSONObject;

/**
   Given a geoJSON feature, mangles it into an ICSS 'Thing'.
 */
public class GeoJSONToThing extends EvalFunc<String> {
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
        if (input == null || input.size() < 1 || input.isNull(0))
            return null;

        String json = input.get(0).toString();
        String result = null;
        try {
            GeoFeature feature = (GeoFeature)reader.decode(json);
            result = feature.toIcssThing();
        } catch (JSONException e) {}
        return result;
    }
}
