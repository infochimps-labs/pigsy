package com.infochimps.hadoop.pig.geo;

import java.io.IOException;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;

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
   Needs to take two bags of arbitrary geoJSON encoded geometries as input. 
   This udf will iterate through each of the geometries of the first bag
   and each of the geometries in the second bag. If a given geometry from the first
   is inside a given geometry in the second then the id of the second is appended to the
   first geometrie's 'within' array inside the geometry's metadata hash.
 */
public final class ContainingGeometries extends EvalFunc<DataBag> {

    private static TupleFactory tupleFactory = TupleFactory.getInstance();
    private static BagFactory   bagFactory = BagFactory.getInstance();
    
    private final class Pair {
	public String featureId;
	public Geometry geometry;
	public Pair( String f, Geometry g) {
	    featureId = f;
	    geometry = g;
	}
    }

    private final MfGeoFactory mfFactory = new MfGeoFactory() {
            public MfFeature createFeature(String id, MfGeometry geometry, JSONObject properties) {
                return new GeoFeature(id, geometry, properties);
            }
        };
    
    private final MfGeoJSONReader reader = new MfGeoJSONReader(mfFactory);

    private final String INSIDE_KEY = "inside";
    private final String INTERSECT_KEY = "intersects";
    
    public DataBag exec(Tuple input) throws IOException {
        if (input == null || input.size() < 2 || input.isNull(0) || input.isNull(1))
            return null;
        DataBag b1 = (DataBag)input.get(0);
        DataBag b2 = (DataBag)input.get(1);
        DataBag returnBag = bagFactory.newDefaultBag();

	// Precalculate the list of geometry objects for the second bag. We store it
	// in a HashMap because we need both the geometry and the featureid. If there
	// are duplicate featureids, only one of the associated geometries will be checked...
	// If this turns out to be a problem, turn geomB2 into an ArrayList< Map.Entry<String,Geometry> >,
	// or something similar and then iterate over the array instead of the hash.
	ArrayList<Pair> geomB2 = new ArrayList<Pair>();
	for(Tuple y : b2) {
	    try {
		if (!y.isNull(0)) {
		    String jsonY = y.get(0).toString();
		    MfGeo resultY = reader.decode(jsonY);
		    GeoFeature featureY = (GeoFeature)resultY;
		    MfGeometry mfGeomY = featureY.getMfGeometry();
		    Geometry geometryY = mfGeomY.getInternalGeometry();
		    geomB2.add( new Pair(featureY.getFeatureId(), geometryY ) );
		}
	    }  catch (JSONException e) {}
	}

        
        for (Tuple x : b1) {
            if (!x.isNull(0)) {
                String jsonX = x.get(0).toString();
                
                // This will hold a list of the ids that this geometry is inside                
                List<String> insideList = new ArrayList<String>();
                // This will hold a list of the ids that this geometry is overlapped by
                // List<String> intersectsList = new ArrayList<String>();
                // Travis
                try {
                    MfGeo resultX = reader.decode(jsonX);
                    GeoFeature featureX = (GeoFeature)resultX;
                    MfGeometry mfGeomX = featureX.getMfGeometry();
                    Geometry geometryX = mfGeomX.getInternalGeometry();

		    // Iterate over the hash of b2 geometry objects to check for 
		    // intersections.
                    for (Pair p : geomB2 )  {
			String featureId = p.featureId;
			Geometry geometryY = p.geometry;
			
			if (geometryY.contains(geometryX)) {
			    insideList.add(featureId);
			}

			// if (geometryY.intersects(geometryX)) {
			//     intersectsList.add(featureId);
			//     if (geometryY.contains(geometryX)) {
			// 	insideList.add(featureId);
			//     }
			// }
			// Travis
                    }
                    JSONObject properties = featureX.getProperties();
                    if (insideList.size() > 0) properties.put(INSIDE_KEY, insideList);
                    // if (intersectsList.size() > 0) properties.put(INTERSECT_KEY, intersectsList); 
		    // Travis
                    GeoFeature newFeatureX = new GeoFeature(featureX.getFeatureId(), mfGeomX, properties);
                    returnBag.add(tupleFactory.newTuple(newFeatureX.serialize()));
                    
                } catch (JSONException e) {}
            }
        }
        
        return returnBag;
    }
}
