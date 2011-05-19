package com.infochimps.hadoop.pig.geo;

import java.io.IOException;
import java.util.List;

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

import com.vividsolutions.jts.geom.Polygon;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.Point;

import org.json.JSONException;
import org.json.JSONObject;

/**
 * All this is going to do is take a geoJSON blob, the geocell that contains it, and search the geocell's children one level.
 * It will return data like the following:
 * <p>
 * {(geocell_1, geometry_within_1), (geocell_2, geometry_within_2), ..., (geocell_n, geometry_within_n)}
 * <p>
 * This way we can perform a massively parallel search of space for all cells for a given tile.
 */
public class SearchGeoCell extends EvalFunc<DataBag> {

    private static TupleFactory tupleFactory = TupleFactory.getInstance();
    private static BagFactory   bagFactory   = BagFactory.getInstance();

    private static int MAX_CELLS             = 500000;
    private static final String GEOM_POINT   = "Point";
    private static final String GEOM_COLLEC  = "GeometryCollection";
    
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
        if (input == null || input.size() < 2 || input.isNull(0) || input.isNull(1))
            return null;

        DataBag returnCells = bagFactory.newDefaultBag();
        String geocell      = input.get(0).toString();
        String jsonBlob     = input.get(1).toString();

        try {
            // All the gunk involved with reading in a 
            MfGeo result       = reader.decode(jsonBlob);
            GeoFeature feature = (GeoFeature)result;
            MfGeometry geom    = feature.getMfGeometry();
            Geometry jts       = geom.getInternalGeometry();

            // If the object is a simple point then act on it immediately and without further adeau
            if (jts.getGeometryType().equals(GEOM_POINT)) {
                Point point      = (Point) jts;
                Tuple newGeoCell = tupleFactory.newTuple(2);
                newGeoCell.set(0, GeoCellUtils.compute(point.getX(), point.getY(), geocell.length() + 1));
                newGeoCell.set(1, jsonBlob);                        
                returnCells.add(newGeoCell);
            } else { // Otherwise check that it's not going to produce too many cells
                int approxCellCount = GeoCellUtils.cellCount(jts.getEnvelopeInternal(), geocell.length() + 1);
                if ( approxCellCount > MAX_CELLS) {
                    System.out.println("Too many cells! ["+approxCellCount+"]");
                    return null;
                }

                List<String> childCells = GeoCellUtils.childrenContaining(jts, geocell);
                for (String child : childCells) {
                    Polygon cellBox = GeoCellUtils.computeBox(child);
                    Geometry cut    = cellBox.intersection(jts);
                   
                    cut = (cut.getGeometryType().equals(GEOM_COLLEC) ? cut.getEnvelope() : cut );
                    
                    MfGeometry snippedGeom    = new MfGeometry(cut);
                    GeoFeature snippedFeature = new GeoFeature(feature.getFeatureId(), snippedGeom, feature.getProperties());

                    Tuple newGeoCell = tupleFactory.newTuple(2);
                    newGeoCell.set(0, child);
                    newGeoCell.set(1, snippedFeature.serialize());
                    returnCells.add(newGeoCell);
                }
            }
        } catch (JSONException e) {}
        catch (com.vividsolutions.jts.geom.TopologyException e) { System.out.println(e.getMessage()); }
        return returnCells;
    }
}
