package com.infochimps.hadoop.pig.geo;

import java.io.IOException;

import org.apache.commons.codec.digest.DigestUtils;

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

public class GetCentroid extends EvalFunc<String> {
    private final MfGeoFactory mfFactory = new MfGeoFactory() {
            public MfFeature createFeature(String id, MfGeometry geometry, JSONObject properties) {
                return new GeoFeature(id, geometry, properties);
            }
        };

    private final MfGeoJSONReader reader = new MfGeoJSONReader(mfFactory);

    private final String REGION = "_region";
    
    public String exec(Tuple input) throws IOException {
        if (input == null || input.size() < 1 || input.isNull(0))
            return null;

        String json = input.get(0).toString();        
        GeoFeature resultFeature = null;
        try {
            MfGeo decoded = reader.decode(json);
            GeoFeature feature = (GeoFeature)decoded;
            MfGeometry mfGeom = feature.getMfGeometry();
            Geometry geometry = mfGeom.getInternalGeometry();
            MfGeometry centroid  = new MfGeometry(geometry.getCentroid());
            resultFeature = new GeoFeature(feature.getFeatureId(), centroid, feature.getProperties().put(REGION, true));            
        } catch (JSONException e) {}
        
        return resultFeature.serialize();
    }
}
