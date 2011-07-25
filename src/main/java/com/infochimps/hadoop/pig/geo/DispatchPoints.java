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
   
   This is very similar to FeatureToQuadkeys in that it takes arbitrary geoJSON
   point object and generates a set of google quadkeys for that geometry at a
   given resolution. The key difference here being that this:
   <p>
   a) Only works for points
   b) Generates the quadkeys of the tiles that the point maps to directly _as well as_
      that of the tile's 8 neighboring tiles.
  <p>
  What this means is that we can incorporate this UDF in a clustering workflow and
  account for the points that fall near the edges of tiles.
  <p>
  Arguments:
  <ul>
  <li><b>resolution</b>: An integer resolution [1-23] specifying the zoom level to get
  quadkeys for.</li>
  <li><b>geoJSON</b>: An arbitrary geoJSON 'Point' object.</li>
  </ul>
  
 */
public class DispatchPoints extends EvalFunc<DataBag> {
    private static TupleFactory tupleFactory = TupleFactory.getInstance();
    private static BagFactory   bagFactory = BagFactory.getInstance();
    private static final String GEOM_POINT = "Point";

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
        Integer resolution = (Integer)input.get(0);
        String jsonBlob = input.get(1).toString();
        try {
            MfGeo result = reader.decode(jsonBlob);
            GeoFeature feature = (GeoFeature)result;
            MfGeometry geom = feature.getMfGeometry();
            Geometry jts = geom.getInternalGeometry();

            // Be sure to only operate on points, fuck you otherwise
            if (jts.getGeometryType().equals(GEOM_POINT)) {
                // There had better be 9 of them
                List<String> keyAndNeighbors = QuadKeyUtils.quadKeyAndNeighbors(((Point)jts).getX(), ((Point)jts).getY(), resolution);
                for (String quadKey : keyAndNeighbors) {
                    Tuple newQuadKey = tupleFactory.newTuple(quadKey);
                    returnKeys.add(newQuadKey);
                }
            } else {/* I regret to inform you, but this only works with points. */}
        } catch (JSONException e) {}
        return returnKeys;
    }
}
