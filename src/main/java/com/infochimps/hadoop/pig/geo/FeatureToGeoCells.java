package com.infochimps.hadoop.pig.geo;

import java.io.IOException;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;

import org.mapfish.geo.MfGeo;
import org.mapfish.geo.MfGeoFactory;
import org.mapfish.geo.MfGeoJSONReader;
import org.mapfish.geo.MfGeometry;
import org.mapfish.geo.MfFeature;

import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.Point;

import org.json.JSONException;
import org.json.JSONObject;

/**
 * An Apache Pig user-defined-function (UDF) designed to generate a set of map tiles (geocells) based on input
 * data in the geoJSON format.
 * <p>
 * The exec function has the following parameters (wrapped by a Tuple):
 * <ul>
 * <li><b>resolution</b>: An integer resolution ranging from 1 to 13 with 13 being the highest.
 * <li><b>blob</b>: A geoJSON "Feature". See the geoJSON specification for what that acutally means.
 * </ul>
 */
public class FeatureToGeoCells extends EvalFunc<DataBag> {

    private static TupleFactory tupleFactory = TupleFactory.getInstance();
    private static BagFactory   bagFactory   = BagFactory.getInstance();

    // Maximum number of geocells to create for a single polygon
    private static int MAX_CELLS             = 500000;
    private static final String GEOM_POINT   = "Point";

    // Simple factory for creating geoJSON features from json strings
    private final MfGeoFactory mfFactory = new MfGeoFactory() {
            public MfFeature createFeature(String id, MfGeometry geometry, JSONObject properties) {
                return new GeoFeature(id, geometry, properties);
            }
        };

    private final MfGeoJSONReader reader = new MfGeoJSONReader(mfFactory);

    /**
     * The function that will be called on every input. Does the meat of the work here.
     * <p>
     * @param input: A pig Tuple. Supplied during runtime. This tuple must have two fields:
     * <ul>
     * <li><b>resolution</b>: An integer resolution ranging from 1 to 13 with 13 being the highest.
     * <li><b>blob</b>: A geoJSON "Feature". See the geoJSON specification for what that acutally means.
     * </ul>
     */
    public DataBag exec(Tuple input) throws IOException {
        if (input == null || input.size() < 2)
            return null;

        DataBag returnCells = bagFactory.newDefaultBag();
        Integer resolution  = (Integer)input.get(0);
        Object jsonObj      = input.get(1);

        // Return an empty bag if no json string is supplied
        if (jsonObj == null) {
            return returnCells;
        }
        
        String jsonBlob       = jsonObj.toString();
        try {
            // All the gunk involved with reading in a 
            MfGeo result       = reader.decode(jsonBlob);
            GeoFeature feature = (GeoFeature)result;
            MfGeometry geom    = feature.getMfGeometry();
            Geometry jts       = geom.getInternalGeometry();

            // If the object is a simple point then act on it immediately and without further adeau
            if (jts.getGeometryType().equals(GEOM_POINT)) {
                Point point   = (Point) jts;
                Tuple geocell = tupleFactory.newTuple(GeoCellUtils.compute(point.getX(), point.getY(), resolution));
                returnCells.add(geocell);
            } else { // Otherwise check that it's not going to produce too many cells
                int approxCellCount = GeoCellUtils.cellCount(jts.getEnvelopeInternal(), resolution);
                if ( approxCellCount > MAX_CELLS) {
                    System.out.println("Too many cells! ["+approxCellCount+"]");
                    return returnCells;
                }
                // Finally, compute all the cells for the polygon
                returnCells = GeoCellUtils.allCellsFor(jts, resolution);
            }
        } catch (JSONException e) {}
        return returnCells;
    }
}
