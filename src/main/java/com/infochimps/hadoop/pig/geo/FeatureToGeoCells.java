package com.infochimps.hadoop.pig.geo;

import java.io.IOException;
import java.lang.Math;
import java.util.List;
import java.util.Map;
import java.util.HashMap;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.impl.util.WrappedIOException;
import org.apache.pig.impl.logicalLayer.schema.Schema;

import org.mapfish.geo.MfGeo;
import org.mapfish.geo.MfGeoFactory;
import org.mapfish.geo.MfGeoJSONReader;
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

import org.json.JSONException;
import org.json.JSONObject;
import org.json.JSONWriter;

public class FeatureToGeoCells extends EvalFunc<DataBag> {

    protected Schema schema_;

    private static TupleFactory tupleFactory = TupleFactory.getInstance();
    private static BagFactory   bagFactory   = BagFactory.getInstance();
    private static int MAX_CELLS             = 500000;
    private static final String GEOM_POINT   = "Point";
    
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
        
        if (jsonObj == null) {
            return returnCells;
        }
        
        String jsonBlob       = jsonObj.toString();

        Double lng            = 0.0;
        Double lat            = 0.0;
        
        try {
            MfGeo result       = reader.decode(jsonBlob);
            GeoFeature feature = (GeoFeature)result;
            MfGeometry geom    = feature.getMfGeometry();
            Geometry jts       = geom.getInternalGeometry();

            if (jts.getGeometryType().equals(GEOM_POINT)) {
                Point point   = (Point) jts;
                Tuple geocell = tupleFactory.newTuple(GeoCellUtils.compute(point.getX(), point.getY(), resolution));
                returnCells.add(geocell);
            } else {
                int approxCellCount = GeoCellUtils.cellCount(jts.getEnvelopeInternal(), resolution);
                if ( approxCellCount > MAX_CELLS) {
                    System.out.println("Too many cells! ["+approxCellCount+"]");
                    return returnCells;
                }
                returnCells = GeoCellUtils.allCellsFor(jts, resolution);
            }
        } catch (JSONException e) {}
        return returnCells;
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

        public MfGeometry getMfGeometry() {
            return geometry;
        }

        public void toJSON(JSONWriter builder) throws JSONException {
            throw new RuntimeException("Not implemented");
        }
    }
}
