package com.infochimps.hadoop.pig.geo;

import java.util.Iterator;
import java.util.ArrayList;
import java.util.List;

import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;

import org.json.JSONObject;
import org.mapfish.geo.MfGeometry;
import com.vividsolutions.jts.geom.*;

public final class QuadKeyUtils {

    private static final int TILE_SIZE = 256;
    private static final double MIN_LATITUDE = -85.05112878;
    private static final double MAX_LATITUDE = 85.05112878;
    private static final double MIN_LONGITUDE = -180;
    private static final double MAX_LONGITUDE = 180;

    public static final double EARTH_RADIUS = 6378137;
    public static final double EARTH_CIRCUM = EARTH_RADIUS * 2.0 * Math.PI;
    public static final double EARTH_HALF_CIRC = EARTH_CIRCUM / 2.0;
    public static final double FULL_RESOLUTION = EARTH_CIRCUM / 256.0;
    
    private static final int MAX_ZOOM_LEVEL = 23;
    private static final int MAX_CELLS = 500000;
    private static final String GEOM_COLLEC = "GeometryCollection";

    private static TupleFactory tupleFactory = TupleFactory.getInstance();
    private static BagFactory bagFactory = BagFactory.getInstance();
    private static final GeometryFactory geomFactory = new GeometryFactory();

    /**
       Get all cells overlapping the given geometry at the specified zoom level.
     */
    public static DataBag allCellsFor(Geometry g, int maxDepth) {
        String container = containingQuadKey(g, maxDepth);

        List<String> keysToCheck = childrenFor(container);

        DataBag returnKeys = bagFactory.newDefaultBag();

        for (String key : keysToCheck) {
            boolean fullySearched = checkQuadKey(key, returnKeys, g, maxDepth);

            //
            // If there are ever too many cells, stop everything and return empty bag
            //
            if (!fullySearched) {
                System.out.println("Too many cells! ["+returnKeys.size()+"]");
                returnKeys.clear();
                return returnKeys;
            }
        }

        //
        System.out.println(serializeBagOfQuadkeys(returnKeys));
        //
        return returnKeys;
    }

    /**
       For debugging purposes
     */
    public static String serializeBagOfQuadkeys(DataBag quadKeys) {
        StringBuffer res = new StringBuffer();
        try {
            res.append("{\"type\":\"FeatureCollection\", \"features\":[");
            Iterator itr = quadKeys.iterator();
            while (itr.hasNext()) {
                Tuple tupleKey = (Tuple)itr.next();
                String quadKey = tupleKey.get(0).toString();
                Polygon bbox = quadKeyToBox(quadKey);
                MfGeometry geom = new MfGeometry(bbox);
                GeoFeature f = new GeoFeature(quadKey, geom, new JSONObject("{\"quadkey\":\""+quadKey+"\"}"));
                res.append(f.serialize());
                if (itr.hasNext()) {
                    res.append(",");
                }
            }
            res.append("]}");
        } catch (Exception e) {
        }
        return res.toString();
    }

    /**
       Recursively search through quadKey for overlapping with the passed in geometry.
     */
    public static boolean checkQuadKey(String quadKey, DataBag returnKeys, Geometry g, int maxDepth) {
        // Compute bounding box for the cell
        Polygon keyBox = quadKeyToBox(quadKey);
        
        if (returnKeys.size() > MAX_CELLS) {
            return false;
        }
       
        if (keyBox.intersects(g)) {

            if (quadKey.length() >= maxDepth ) {
                Tuple quadKeyTuple = tupleFactory.newTuple(quadKey);
                returnKeys.add(quadKeyTuple);
                return true;
            } 
            List<String> children = childrenFor(quadKey);
            
            Geometry cut = g.intersection(keyBox);
            cut = (cut.getGeometryType().equals(GEOM_COLLEC) ? cut.getEnvelope() : cut );
            
            for (String child : children) {
                checkQuadKey(child, returnKeys, cut, maxDepth);
            }
        }
        return true;
    }
    
    public static List<String> childrenFor(String quadKey) {
        int res = quadKey.length();
        List<String> children = new ArrayList<String>();
        StringBuilder child = new StringBuilder(quadKey);
        for (int i = 0; i < 4; i++) {
            child.append(i);
            children.add(child.toString());
            child.deleteCharAt(res);
        }
        return children;
    }


    public static int maxTileAtZoom(int levelOfDetail) {
        Double maxTile = (Math.pow(2.0, levelOfDetail) - 1); 
        return maxTile.intValue();
    }

    /**
       Computes the bounding box of a quadKey.
     */
    public static Polygon quadKeyToBox(String quadKey) {

        int[] tileXY = quadKeyToTileXY(quadKey);
        int[] pixelXYMin = tileXYToPixelXY(tileXY[0], tileXY[1]);
        int[] pixelXYMax = {pixelXYMin[0] + 255, pixelXYMin[1] + 255};

        //convert to latitude and longitude coordinates
        double west = pixelXYMin[0]*360.0/(256.0*Math.pow(2.0,quadKey.length())) - 180.0; 
        double north = Math.asin((Math.exp((0.5 - pixelXYMin[1] / 256.0 / Math.pow(2.0,quadKey.length())) * 4 * Math.PI) - 1) / (Math.exp((0.5 - pixelXYMin[1] / 256.0 / Math.pow(2.0,quadKey.length())) * 4 * Math.PI) + 1)) * 180 / Math.PI;

        double east = pixelXYMax[0]*360.0/(256.0*Math.pow(2.0,quadKey.length())) - 180.0;
        double south = Math.asin((Math.exp((0.5 - pixelXYMax[1] / 256.0 / Math.pow(2.0,quadKey.length())) * 4 * Math.PI) - 1) / (Math.exp((0.5 - pixelXYMax[1] / 256.0 / Math.pow(2.0,quadKey.length())) * 4 * Math.PI) + 1)) * 180 / Math.PI;

        Coordinate nw = new Coordinate(west, north);
        Coordinate ne = new Coordinate(east, north);
        Coordinate sw = new Coordinate(west, south);
        Coordinate se = new Coordinate(east, south);    
        Coordinate[] bboxCoordinates = {nw, ne, se, sw, nw};    
        LinearRing bboxRing = geomFactory.createLinearRing(bboxCoordinates);
        Polygon bbox_poly = geomFactory.createPolygon(bboxRing, null);
        return bbox_poly;
    } 
 
    /**
       The desired behavior is to return an empty string if the geometry is
       too large to be inside even the lowest resolution quadKey.
     */
    public static String containingQuadKey(Geometry g, int levelOfDetail) {
        Point centroid = g.getCentroid();
        for (int i = levelOfDetail; i > 0; i--) {
            String quadKey = geoPointToQuadKey(centroid.getX(), centroid.getY(), i);
            Polygon quadKeyBox = quadKeyToBox(quadKey);
            if (quadKeyBox.contains(g)) return quadKey;
        }
        return "";
    }

    /**
     * Determines the map width and height (in pixels) at a specified level of detail.
     * 
     * @param levelOfDetail
     *            Level of detail, from 1 (lowest detail) to 23 (highest detail)
     * @return The map width and height in pixels
     */

    public static int mapSize(final int levelOfDetail) {
        return TILE_SIZE << levelOfDetail;
    }

    
    /**
     * Clips a number to the specified minimum and maximum values.
     * 
     * @param n
     *            The number to clip
     * @param minValue
     *            Minimum allowable value
     * @param maxValue
     *            Maximum allowable value
     * @return The clipped value.
     */
    private static double clip(final double n, final double minValue, final double maxValue) {
        return Math.min(Math.max(n, minValue), maxValue);
    }


    public static String geoPointToQuadKey(double longitude, double latitude, final int levelOfDetail) {
        int[] pixelXY = geoPointToPixelXY(longitude, latitude, levelOfDetail);
        int[] tileXY = pixelXYToTileXY(pixelXY[0], pixelXY[1]);
        return tileXYToQuadKey(tileXY[0], tileXY[1], levelOfDetail);
    }
    
    /**
     * Converts a point from latitude/longitude WGS-84 coordinates (in degrees) into pixel XY
     * coordinates at a specified level of detail.
     * 
     * @param latitude
     *            Latitude of the point, in degrees
     * @param longitude
     *            Longitude of the point, in degrees
     * @param levelOfDetail
     *            Level of detail, from 1 (lowest detail) to 23 (highest detail)
     * @param reuse
     *            An optional Point to be recycled, or null to create a new one automatically
     * @return Output parameter receiving the X and Y coordinates in pixels
     */
    public static int[] geoPointToPixelXY(double longitude, double latitude, final int levelOfDetail) {
        latitude = clip(latitude, MIN_LATITUDE, MAX_LATITUDE);
        longitude = clip(longitude, MIN_LONGITUDE, MAX_LONGITUDE);

        final double x = (longitude + 180) / 360;
        final double sinLatitude = Math.sin(latitude * Math.PI / 180);
        final double y = 0.5 - Math.log((1 + sinLatitude) / (1 - sinLatitude)) / (4 * Math.PI);

        final int mapSize = mapSize(levelOfDetail);
        int[] pixelXY = {(int) clip(x * mapSize + 0.5, 0, mapSize - 1), (int) clip(y * mapSize + 0.5, 0, mapSize - 1)};
        return pixelXY;
    }

    /**
     * Converts pixel XY coordinates into tile XY coordinates of the tile containing the specified
     * pixel.
     * 
     * @param pixelX
     *            Pixel X coordinate
     * @param pixelY
     *            Pixel Y coordinate
     * @param reuse
     *            An optional Point to be recycled, or null to create a new one automatically
     * @return Output parameter receiving the tile X and Y coordinates
     */
    public static int[] pixelXYToTileXY(final int pixelX, final int pixelY) {
        int[] tileXY = {pixelX / TILE_SIZE, pixelY / TILE_SIZE};
        return tileXY;
    }
    
    /**
     * Converts tile XY coordinates into pixel XY coordinates of the upper-left pixel of the
     * specified tile.
     * 
     * @param tileX
     *            Tile X coordinate
     * @param tileY
     *            Tile X coordinate
     * @param reuse
     *            An optional Point to be recycled, or null to create a new one automatically
     * @return Output parameter receiving the pixel X and Y coordinates
     */
    public static int[] tileXYToPixelXY(final int tileX, final int tileY) {
        int[] pixelXY = {tileX * TILE_SIZE, tileY * TILE_SIZE};
        return pixelXY;
    }


    /**
     * Converts tile XY coordinates into a QuadKey at a specified level of detail.
     * 
     * @param tileX
     *            Tile X coordinate
     * @param tileY
     *            Tile Y coordinate
     * @param levelOfDetail
     *            Level of detail, from 1 (lowest detail) to 23 (highest detail)
     * @return A string containing the QuadKey
     */
    public static String tileXYToQuadKey(final int tileX, final int tileY, final int levelOfDetail) {
        final StringBuilder quadKey = new StringBuilder();
        for (int i = levelOfDetail; i > 0; i--) {
            char digit = '0';
            final int mask = 1 << (i - 1);
            if ((tileX & mask) != 0) {
                digit++;
            }
            if ((tileY & mask) != 0) {
                digit++;
                digit++;
            }
            quadKey.append(digit);
        }
        return quadKey.toString();
    }


    /**
     * Converts a QuadKey into tile XY coordinates.
     * 
     * @param quadKey
     *            QuadKey of the tile
     * @param reuse
     *            An optional Point to be recycled, or null to create a new one automatically
     * @return Output parameter receiving the tile X and y coordinates
     */
    public static int[] quadKeyToTileXY(final String quadKey) {
        int tileX = 0;
        int tileY = 0;

        final int levelOfDetail = quadKey.length();
        for (int i = levelOfDetail; i > 0; i--) {
            final int mask = 1 << (i - 1);
            switch (quadKey.charAt(levelOfDetail - i)) {
            case '0':
                break;
            case '1':
                tileX |= mask;
                break;
            case '2':
                tileY |= mask;
                break;
            case '3':
                tileX |= mask;
                tileY |= mask;
                break;

            default:
                throw new IllegalArgumentException("Invalid QuadKey digit sequence.");
            }
        }
        int[] tileXY = {tileX, tileY};
        return tileXY;
    }

}
