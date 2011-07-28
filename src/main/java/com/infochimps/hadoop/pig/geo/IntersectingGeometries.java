package com.infochimps.hadoop.pig.geo;

import java.io.IOException;
import java.util.List;
import java.util.ArrayList;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;

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

/**
   
   Takes a single bag of geoJSON entities. Iterates through each entity
   in the bag and compares it to every other entity. A list of overlapping
   ids is accumulated. Returns a bag, {(geometry_id, intersecting_id)}, where
   intersecting_id is the id of a geometry that intersects with geometry_id.
   
 */
public final class IntersectingGeometries extends EvalFunc<DataBag> {

    private static TupleFactory tupleFactory = TupleFactory.getInstance();
    private static BagFactory   bagFactory = BagFactory.getInstance();
    
    private final MfGeoFactory mfFactory = new MfGeoFactory() {
            public MfFeature createFeature(String id, MfGeometry geometry, JSONObject properties) {
                return new GeoFeature(id, geometry, properties);
            }
        };
    
    private final MfGeoJSONReader reader = new MfGeoJSONReader(mfFactory);
    
    public DataBag exec(Tuple input) throws IOException {
        if (input == null || input.size() < 1 || input.isNull(0))
            return null;
        DataBag jsonBag = (DataBag)input.get(0);
        DataBag returnBag = bagFactory.newDefaultBag();
        
        for (Tuple x : jsonBag) {
            if (!x.isNull(0)) {
                String jsonX = x.get(0).toString();
                
                try {
                    GeoFeature featureX = (GeoFeature)reader.decode(jsonX);
                    MfGeometry mfGeomX = featureX.getMfGeometry();
                    Geometry geometryX = mfGeomX.getInternalGeometry();
                    for (Tuple y : jsonBag)  {
                        if (x!=y && !y.isNull(0)) {
                            String jsonY = y.get(0).toString();
                            GeoFeature featureY = (GeoFeature)reader.decode(jsonY);
                            MfGeometry mfGeomY = featureY.getMfGeometry();
                            Geometry geometryY = mfGeomY.getInternalGeometry();
                            if (geometryY.overlaps(geometryX)) {
                                Tuple out = tupleFactory.newTuple(2);
                                out.set(0, featureX.getFeatureId());
                                out.set(1, featureY.getFeatureId());
                                returnBag.add(out);
                            }
                        }
                    }
                } catch (JSONException e) {}
            }
        }
        
        return returnBag;
    }
}
