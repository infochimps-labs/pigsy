package com.infochimps.hadoop.pig.geo;

import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;

import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Polygon;
import com.vividsolutions.jts.geom.LinearRing;

public final class QuadKeyUtils {
    public static String compute(double lng, double lat, int zoom) {
        return "Hi mom!";
    }

    /**
     * Count the number of cells that would be contained within the bounding box
     * at the resolution specified
     */
    public static int cellCount(Envelope bbox, int zoom) {
        return 0;
    }

    public static DataBag allCellsFor(Geometry g, int maxDepth) {
        return null;
    }
}
