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

/**
   
   Given a qualifier (namespace + '.' + protocol + '.' + layer), a domain id field name (eg. 'geoname_id')
   and a geoJSON entity, generates a guid for it. To do this the method parses the geoJSON, pulls out
   the value of the named id field from the properties HashMap and concatenates the qualifier and the
   domain id. If there is no domain id use a '-1'.
   
 */
public class AttachGUID extends EvalFunc<String> {
    private final MfGeoFactory mfFactory = new MfGeoFactory() {
            public MfFeature createFeature(String id, MfGeometry geometry, JSONObject properties) {
                return new GeoFeature(id, geometry, properties);
            }
        };

    private final MfGeoJSONReader reader = new MfGeoJSONReader(mfFactory);

    private final String COLON = ":";
    private final String CHARSET = "UTF-8";
    
    public String exec(Tuple input) throws IOException {
        if (input == null || input.size() < 3 || input.isNull(0) || input.isNull(1) || input.isNull(2))
            return null;

        String qualifier = input.get(0).toString();
        String id_field = input.get(1).toString(); // if this is -1, then the records are assumed to have no domain id. The full json of the object itself is used.
        String json = input.get(2).toString();
        String resultId = null;
        String result = null;
        GeoFeature resultFeature = null;
        try {
            MfGeo decoded = reader.decode(json);
            GeoFeature feature = (GeoFeature)decoded;
            Object domainId = feature.getProperties().get(id_field);
            if (domainId!=null) {
                resultId = constructGUID(qualifier, domainId.toString());
            } else {
                resultId = constructGUID(qualifier, json);
            }
            resultFeature = new GeoFeature(resultId, feature.getMfGeometry(), feature.getProperties());
            result = resultFeature.serialize();
        } catch (JSONException e) {
            e.printStackTrace();
        }
        
        return result;
    }

    private String constructGUID(String qualifier, String domainId) {
        String result = null;
        try {
            StringBuffer buffer = new StringBuffer();
            buffer.append(qualifier);
            buffer.append(COLON);
            buffer.append(domainId);
            result = DigestUtils.md5Hex(buffer.toString().getBytes(CHARSET));
        } catch (Exception e) {}
        return result;
    }
}
