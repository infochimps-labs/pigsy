package com.infochimps.hadoop.pig.geo;

import java.io.IOException;

import java.util.List;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.backend.executionengine.ExecException;

import org.mapfish.geo.MfGeo;
import org.mapfish.geo.MfGeoFactory;
import org.mapfish.geo.MfGeoJSONReader;
import org.mapfish.geo.MfGeoJSONWriter;
import org.mapfish.geo.MfGeometry;
import org.mapfish.geo.MfFeature;

import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.geom.Polygon;

import org.json.JSONStringer;
import org.json.JSONException;
import org.json.JSONObject;

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
            String serializedFeature = feature.serialize();
            
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
            } else if (field1 instanceof String) {
                result = ((String)field1).compareTo((String)field2);
            }
            return result;
        }
        
    }
}
