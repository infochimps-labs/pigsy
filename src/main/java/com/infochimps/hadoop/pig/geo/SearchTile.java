package com.infochimps.hadoop.pig.geo;

import java.io.IOException;
import java.util.List;

import org.mapfish.geo.MfGeo;
import org.mapfish.geo.MfGeoFactory;
import org.mapfish.geo.MfGeoJSONReader;
import org.mapfish.geo.MfGeometry;
import org.mapfish.geo.MfFeature;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;

import com.vividsolutions.jts.geom.Polygon;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.Point;

import org.json.JSONException;
import org.json.JSONObject;

/**
 * All this is going to do is take a geoJSON blob, the quadkey that contains it, and search the quadkeys's children one level.
 * It will return data like the following:
 * <p>
 * {(quadkey_1, geometry_within_1), (quadkey_2, geometry_within_2), ..., (quadkey_n, geometry_within_n)}
 * <p>
 * This way we can perform a massively parallel (and memory efficient) search of the space in a tile.
 */
public class SearchTile extends EvalFunc<DataBag> {

    private static TupleFactory tupleFactory = TupleFactory.getInstance();
    private static BagFactory   bagFactory = BagFactory.getInstance();
    private static final String GEOM_POINT = "Point";
    private static final String GEOM_COLLEC  = "GeometryCollection";

    // Simple factory for creating geoJSON features from json strings
    private final MfGeoFactory mfFactory = new MfGeoFactory() {
            public MfFeature createFeature(String id, MfGeometry geometry, JSONObject properties) {
                return new GeoFeature(id, geometry, properties);
            }
        };

    private final MfGeoJSONReader reader = new MfGeoJSONReader(mfFactory);

    public DataBag exec(Tuple input) throws IOException {
        if (input == null || input.size() < 2)
            return null;

        DataBag returnKeys = bagFactory.newDefaultBag();
        String quadKey = input.get(0).toString();
        String jsonBlob = input.get(1).toString();

        try {
            // All the gunk involved with reading in a 
            MfGeo result = reader.decode(jsonBlob);
            GeoFeature feature = (GeoFeature)result;
            MfGeometry geom = feature.getMfGeometry();
            Geometry jts = geom.getInternalGeometry();

            // If the object is a simple point then act on it immediately and without further adeau
            if (jts.getGeometryType().equals(GEOM_POINT)) {
                Tuple newQuadKey = tupleFactory.newTuple(2);
                newQuadKey.set(0, QuadKeyUtils.geoPointToQuadKey(((Point)jts).getX(), ((Point)jts).getY(), quadKey.length() + 1));
                newQuadKey.set(1, jsonBlob);                        
                returnKeys.add(newQuadKey);
            } else { // Otherwise check that it's not going to produce too many cells
                List<String> childKeys = QuadKeyUtils.childrenContaining(jts, quadKey);
                for (String child : childKeys) {
                    Polygon quadKeyBox = QuadKeyUtils.quadKeyToBox(child);
                    Geometry cut = quadKeyBox.intersection(jts);
                   
                    cut = (cut.getGeometryType().equals(GEOM_COLLEC) ? cut.getEnvelope() : cut );
                    
                    MfGeometry snippedGeom = new MfGeometry(cut);
                    GeoFeature snippedFeature = new GeoFeature(feature.getFeatureId(), snippedGeom, feature.getProperties());

                    Tuple newQuadKey = tupleFactory.newTuple(2);
                    newQuadKey.set(0, child);
                    newQuadKey.set(1, snippedFeature.serialize());
                    returnKeys.add(newQuadKey);
                }
            }
        } catch (JSONException e) {}
        catch (com.vividsolutions.jts.geom.TopologyException e) { System.out.println(e.getMessage()); }
        return returnKeys;
    }
}
