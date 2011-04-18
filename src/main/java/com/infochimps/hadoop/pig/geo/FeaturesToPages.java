package com.infochimps.hadoop.pig.geo;

import java.io.IOException;
import java.lang.Math;

import java.util.Iterator;
import java.util.List;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.impl.util.WrappedIOException;
import org.apache.pig.impl.logicalLayer.schema.Schema;

import org.mapfish.geo.MfGeo;
import org.mapfish.geo.MfGeoFactory;
import org.mapfish.geo.MfGeoJSONReader;
import org.mapfish.geo.MfGeoJSONWriter;
import org.mapfish.geo.MfGeometry;
import org.mapfish.geo.MfFeature;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryCollection;
import com.vividsolutions.jts.geom.LineString;
import com.vividsolutions.jts.geom.MultiLineString;
import com.vividsolutions.jts.geom.MultiPoint;
import com.vividsolutions.jts.geom.MultiPolygon;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.geom.Polygon;

import org.json.JSONStringer;
import org.json.JSONException;
import org.json.JSONObject;
import org.json.JSONArray;
import org.json.JSONWriter;

public class FeaturesToPages extends EvalFunc<DataBag> {

    private static TupleFactory tupleFactory = TupleFactory.getInstance();
    private static BagFactory   bagFactory   = BagFactory.getInstance();
    private static String NO_SORT_FIELD      = "-1";
    
    private final MfGeoFactory mfFactory = new MfGeoFactory() {
        public MfFeature createFeature(String id, MfGeometry geometry, JSONObject properties) {
            return new GeoFeature(id, geometry, properties);
        }
    };

    private final MfGeoJSONReader reader = new MfGeoJSONReader(mfFactory);
    
    public DataBag exec(Tuple input) throws IOException {
        if (input == null || input.size() < 4 || input.isNull(0) || input.isNull(1) || input.isNull(2) || input.isNull(3)) {
            return null;
        }

        // Arguments
        Integer approxCharsPerPage = (Integer)input.get(0);
        String sortField   = input.get(1).toString();
        String geocell     = input.get(2).toString();
        DataBag bagOfBlobs = (DataBag)input.get(3);

        // Comparator for sorting the result pages
        GeoFeatureComparator featureComp = new GeoFeatureComparator(sortField);
        
        if (bagOfBlobs.size() == 0) {
            return null;
        }

        Polygon cellBox = GeoCellUtils.computeBox(geocell);
        List<GeoFeature> geoFeatures = new ArrayList<GeoFeature>();

        for (Tuple blob : bagOfBlobs) {
            if (!blob.isNull(0)) {
                String jsonBlob = blob.get(0).toString();
                try {
                    MfGeo result = reader.decode(jsonBlob);
                    GeoFeature feature = (GeoFeature)result;
                    MfGeometry geom    = feature.getMfGeometry();
                    Geometry jts       = geom.getInternalGeometry();
                    String geomType    = jts.getGeometryType();
                    if (geomType.equals("Point")) {
                        geoFeatures.add(feature);
                    } else {
                        // snip the feature to fit within the box
                        MfGeometry snippedGeom    = new MfGeometry(cellBox.intersection(jts));
                        GeoFeature snippedFeature = new GeoFeature(feature.getFeatureId(), snippedGeom, feature.getProperties());
                        geoFeatures.add(snippedFeature);
                    }
                } catch (JSONException e) {}                
            }
        }
        
        if (!sortField.equals(NO_SORT_FIELD)) {
            Collections.sort(geoFeatures, featureComp);
        }
        DataBag bagOfPages = bagFeatures(geoFeatures, approxCharsPerPage);
        return bagOfPages;
    }

    public DataBag bagFeatures(List<GeoFeature> sortedPages, int approxCharsPerPage) throws ExecException {
        StringBuilder content = new StringBuilder();
        DataBag bagOfPages    = bagFactory.newDefaultBag();
        Integer pageNum       = 0;
        for (int i = 0; i < sortedPages.size(); i++) {
            
            GeoFeature feature       = sortedPages.get(i);
            String serializedFeature = serializeFeature(feature);
            
            if (serializedFeature != null ) {
                content.append(serializedFeature);
            }
            if ( (i != sortedPages.size()-1) && (content.length() > 0) && (content.length() < approxCharsPerPage)) {
                content.append(',');
            }
            if (content.length() >= approxCharsPerPage) {
                Tuple onePage = tupleFactory.newTuple(2);
                onePage.set(0, pageNum.toString());
                onePage.set(1, content.toString());
                bagOfPages.add(onePage);
                pageNum += 1;
                content = new StringBuilder();
            }
        }

        // In the case where all items fit on one page and are less characters than 'approxCharsPerPage'
        if (content.length() >= 0) {
            Tuple onePage = tupleFactory.newTuple(2);
            onePage.set(0, pageNum.toString());
            onePage.set(1, content.toString());
            String out = "";
            bagOfPages.add(onePage);
        }
        
        return bagOfPages;
    }
    
    public String serializeFeature(GeoFeature feature) {
        JSONStringer stringer   = new JSONStringer();
        MfGeoJSONWriter builder = new MfGeoJSONWriter(stringer);
        try {
            builder.encodeFeature(feature);
        } catch (JSONException e) {
            return null;
        }
        return stringer.toString();
    }
    
    public class GeoFeatureComparator implements Comparator<GeoFeature> {
        private String sortField;
        public GeoFeatureComparator(String sortField) {
            this.sortField = sortField;
        }

        public int compare(GeoFeature f1, GeoFeature f2) {
            Object field1 = f1.getFromProperties(sortField);
            Object field2 = f2.getFromProperties(sortField);
            int result    = 0;
            if (field1 instanceof Integer) {
                result = ((Integer)field1).compareTo((Integer)field2);
            } else if (field1 instanceof Long) {
                result = ((Long)field1).compareTo((Long)field2);
            } else if (field1 instanceof Float) {
                result = ((Float)field1).compareTo((Float)field2);
            } else if (field1 instanceof Float) {
                result = ((Double)field1).compareTo((Double)field2);
            } else {
                result = ((String)field1).compareTo((String)field2);
            }
            return result;
        }
        
    }
    
    // Foobar feature class that MfJSONReader needs
    private class GeoFeature extends MfFeature {
        private final String id;
        private final MfGeometry geometry;
        private final JSONObject properties;

        public GeoFeature(String id, MfGeometry geometry, JSONObject properties) {
            this.id = id;
            this.geometry = geometry;
            this.properties = properties;
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
}
