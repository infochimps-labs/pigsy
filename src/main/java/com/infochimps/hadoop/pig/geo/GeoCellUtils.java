package com.infochimps.hadoop.pig.geo;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

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

public final class GeoCellUtils {

    public static final int GEOCELL_GRID_SIZE    = 4;
    private static final String GEOCELL_ALPHABET = "0123456789abcdef";
    private static final String GEOM_COLLEC      = "GeometryCollection";
    private static final int MAX_CELLS           = 500000;
    private static final GeometryFactory geomFactory = new GeometryFactory();

    private static TupleFactory tupleFactory = TupleFactory.getInstance();
    private static BagFactory   bagFactory   = BagFactory.getInstance();
    
    private static final int[] NORTHWEST = new int[] {-1,1};
    private static final int[] NORTH = new int[] {0,1};
    private static final int[] NORTHEAST = new int[] {1,1};
    private static final int[] EAST = new int[] {1,0};
    private static final int[] SOUTHEAST = new int[] {1,-1};
    private static final int[] SOUTH = new int[] {0,-1};
    private static final int[] SOUTHWEST = new int[] {-1,-1};
    private static final int[] WEST = new int[] {-1,0};


    /**       
       Given a (lng,lat) pair and resolution, computes the geo_cell that contains the coordinates.

     * @param lng: The longitude (x coordinate).
     * @param lat: The latitude  (y coordinate).
     * @param zoom: The zoom level (resolution), ranges from 1 to 13.
     * @return The geo_cell string that the parameters map to.
       
     */
    public static String compute(double lng, double lat, int zoom) {
        float north = 90.0f;
        float south = -90.0f;
        float east  = 180.0f;
        float west  = -180.0f;
        
        StringBuilder cell = new StringBuilder();
        while(cell.length() < zoom) {
            float subcellLonSpan = (east - west)   / GEOCELL_GRID_SIZE;
            float subcellLatSpan = (north - south) / GEOCELL_GRID_SIZE;

            int x = Math.min((int)(GEOCELL_GRID_SIZE * (lng - west)  / (east - west)), GEOCELL_GRID_SIZE - 1);
            int y = Math.min((int)(GEOCELL_GRID_SIZE * (lat - south) / (north - south)), GEOCELL_GRID_SIZE - 1);
            int pair[] = {x,y};
            cell.append(char_for_xy(pair));
            
            south += subcellLatSpan * y;
            north = south + subcellLatSpan;

            west += subcellLonSpan * x;
            east = west + subcellLonSpan;
        }
        return cell.toString(); 
    }

    public static char char_for_xy(int[] pos) {
        return GEOCELL_ALPHABET.charAt(
                (pos[1] & 2) << 2 |
                (pos[0] & 2) << 1 |
                (pos[1] & 1) << 1 |
                (pos[0] & 1) << 0);
    }
    
    public static int[] xy_for_char(char char_) {
        // NOTE: This only works for grid size 4.
        int charI = GEOCELL_ALPHABET.indexOf(char_);
        return new int[] {(charI & 4) >> 1 | (charI & 1) >> 0,
        (charI & 8) >> 2 | (charI & 2) >> 1};
    }

    /**
     *
     * 	 Calculates the grid of cells formed between the two given cells.

      Generates the set of cells in the grid created by interpolating from the
      given Northeast geocell to the given Southwest geocell.

      Assumes the Northwest geocell is actually Northwest of Southeast geocell.

     *
     * @param cellNE: The Northeast geocell string.
     * @param cellSW: The Southwest geocell string.
     * @return A list of geocell strings in the interpolation.
     */
    public static List<String> interpolate(String cellNW, String cellSE) {
        // 2D array, will later be flattened.
        LinkedList<LinkedList<String>> cellSet = new LinkedList<LinkedList<String>>();
        LinkedList<String> cellFirst = new LinkedList<String>();
        cellFirst.add(cellSE);
        cellSet.add(cellFirst);

        // First head west, accumulating all geocells along the way
        while(!collinear(cellFirst.getLast(), cellNW, true)) {
            String cellTmp = adjacent(cellFirst.getLast(), WEST);
            if(cellTmp == null) {
                break;
            }
            cellFirst.add(cellTmp);
        }

        // Then get adjacent geocells northwards.
        while(!cellSet.getLast().getLast().equalsIgnoreCase(cellNW)) {
            LinkedList<String> cellTmpRow = new LinkedList<String>();
            for(String g : cellSet.getLast()) {
                cellTmpRow.add(adjacent(g, NORTH));
            }
            if(cellTmpRow.getFirst() == null) {
                break;
            }
            cellSet.add(cellTmpRow);
        }

        // Flatten cellSet, since it's currently a 2D array.
        List<String> result = new ArrayList<String>();
        for(LinkedList<String> list : cellSet) {
            result.addAll(list);
        }
        return result;
    }

    /**
     * Determines whether the given cells are collinear along a dimension.

     Returns True if the given cells are in the same row (columnTest=False)
     or in the same column (columnTest=True).

     * @param cell1: The first geocell string.
     * @param cell2: The second geocell string.
     * @param columnTest: A boolean, where False invokes a row collinearity test
     and 1 invokes a column collinearity test.
     * @return A bool indicating whether or not the given cells are collinear in the given
     dimension.
    */
    public static boolean collinear(String cell1, String cell2, boolean columnTest) {

        for(int i = 0; i < Math.min(cell1.length(), cell2.length()); i++) {
            int l1[] = xy_for_char(cell1.charAt(i));
            int x1 = l1[0];
            int y1 = l1[1];
            int l2[] = xy_for_char(cell2.charAt(i));
            int x2 = l2[0];
            int y2 = l2[1];

            // Check row collinearity (assure y's are always the same).
            if (!columnTest && y1 != y2) {
                return false;
            }
            // Check column collinearity (assure x's are always the same).
            if(columnTest && x1 != x2) {
                return false;
            }
        }
        return true;
    }


    /**
     * Calculates the geocell adjacent to the given cell in the given direction.
     *
     * @param cell: The geocell string whose neighbor is being calculated.
     * @param dir: An (x, y) tuple indicating direction, where x and y can be -1, 0, or 1.
            -1 corresponds to West for x and South for y, and
             1 corresponds to East for x and North for y.
            Available helper constants are NORTH, EAST, SOUTH, WEST,
            NORTHEAST, NORTHWEST, SOUTHEAST, and SOUTHWEST.
     * @return The geocell adjacent to the given cell in the given direction, or None if
        there is no such cell.

     */
    public static String adjacent(String cell, int[] dir) {
        if(cell == null) {
            return null;
        }
        int dx = dir[0];
        int dy = dir[1];
        char[] cellAdjArr = cell.toCharArray(); // Split the geocell string characters into a list.
        int i = cellAdjArr.length - 1;

        while(i >= 0 && (dx != 0 || dy != 0)) {
            int l[]= xy_for_char(cellAdjArr[i]);
            int x = l[0];
            int y = l[1];

            // Horizontal adjacency.
            if(dx == -1) {  // Asking for left.
                if(x == 0) {  // At left of parent cell.
                    x = GEOCELL_GRID_SIZE - 1;  // Becomes right edge of adjacent parent.
                } else {
                    x--;  // Adjacent, same parent.
                    dx = 0; // Done with x.
                }
            }
            else if(dx == 1) { // Asking for right.
                if(x == GEOCELL_GRID_SIZE - 1) { // At right of parent cell.
                    x = 0;  // Becomes left edge of adjacent parent.
                } else {
                    x++;  // Adjacent, same parent.
                    dx = 0;  // Done with x.
                }
            }

            // Vertical adjacency.
            if(dy == 1) { // Asking for above.
                if(y == GEOCELL_GRID_SIZE - 1) {  // At top of parent cell.
                    y = 0;  // Becomes bottom edge of adjacent parent.
                } else {
                    y++;  // Adjacent, same parent.
                    dy = 0;  // Done with y.
                }
            } else if(dy == -1) {  // Asking for below.
                if(y == 0) { // At bottom of parent cell.
                    y = GEOCELL_GRID_SIZE - 1; // Becomes top edge of adjacent parent.
                } else {
                    y--;  // Adjacent, same parent.
                    dy = 0;  // Done with y.
                }
            }

            int l2[] = {x,y};
            cellAdjArr[i] = char_for_xy(l2);
            i--;
        }
        // If we're not done with y then it's trying to wrap vertically,
        // which is a failure.
        if(dy != 0) {
            return null;
        }

        // At this point, horizontal wrapping is done inherently.
        return new String(cellAdjArr);
    }


    /**
     * Calculates the bounding box of the given geo_cell string as a polygon.
     *
     * @param cell_: The geocell string whose bounding box is to be found
     * @return  The polygon object that describes the bouding box of the
                passed in geo_cell.
     */
    public static Polygon computeBox(String cell_) {
        if(cell_ == null) {
            return null;
        }

        double bbox[][] = {{-180.0, 90.0},{180.0, -90.0}};
        StringBuilder cell = new StringBuilder(cell_);
        while(cell.length() > 0) {
            double subcellLonSpan = (bbox[1][0] - bbox[0][0]) / GEOCELL_GRID_SIZE;
            double subcellLatSpan = (bbox[0][1] - bbox[1][1]) /  GEOCELL_GRID_SIZE;

            int xy[] = xy_for_char(cell.charAt(0));
            int x = xy[0];
            int y = xy[1];

            double[] latestNW = {bbox[0][0]  + subcellLonSpan * x, bbox[1][1] + subcellLatSpan * (y + 1)};
            double[] latestSE = {bbox[0][0]  + subcellLonSpan * (x + 1), bbox[1][1] + subcellLatSpan * y};

            bbox[0] = latestNW;
            bbox[1] = latestSE;
            
            cell.deleteCharAt(0);
        }

        Coordinate nw = new Coordinate(bbox[0][0], bbox[0][1]);
        Coordinate ne = new Coordinate(bbox[1][0], bbox[0][1]);
        Coordinate sw = new Coordinate(bbox[0][0], bbox[1][1]);
        Coordinate se = new Coordinate(bbox[1][0], bbox[1][1]);
        
        Coordinate[] bboxCoordinates = {nw, ne, se, sw, nw};
        LinearRing bboxRing          = geomFactory.createLinearRing(bboxCoordinates);
        Polygon bbox_poly            = geomFactory.createPolygon(bboxRing, null);
        
        return bbox_poly;
    }

    /**
     * Recursively calculates _all_ geocells for a given geometry at the resolution specified.
     
       There are a couple optimizations here:

       1. We recursively search from lowest possble resolution to the highest resolution specified.
          Only the children of cells that actually intersect the geometry are searched.
       2. If the geocell to be searched is completely inside the geometry, we only recurse one
          level deeper. For example, if we are far inside North America, and at sufficiently high
          resolution, we no longer actually care about the fact that we're inside North America.
       
     * @param g: The geometry object to consider
     * @param maxDepth: the maximum resolution to consider
     * @return  A list of geocells.
     */
    public static DataBag allCellsFor(Geometry g, int maxDepth) {

        // Get bounding box of geometry
        Envelope bbox = g.getEnvelopeInternal();

        // Get top left and lower right geocells, at lowest resolution
        String cellNW = compute(bbox.getMinX(), bbox.getMaxY(), 1);
        String cellSE = compute(bbox.getMaxX(), bbox.getMinY(), 1);

        // Fetch all cells within the bounding box, at lowest resolution
        List<String> cellsToCheck    = interpolate(cellNW, cellSE);

        // A pig databag of return cells
        DataBag returnCells = bagFactory.newDefaultBag();
        
        //
        // Finally, recursively search the cells and their children,
        // and their children's children, ...
        //
        for (String cell : cellsToCheck) {
            boolean fullySearched = checkCell(cell, returnCells, g, maxDepth);

            //
            // If there are ever too many cells, stop everything and return empty bag
            //
            if (!fullySearched) {
                System.out.println("Too many cells! ["+returnCells.size()+"]");
                returnCells.clear();
                return returnCells;
            }
        }
        return returnCells;
    }

    /**
      * Recursively search a cell and its children, up to maxDepth, for intersections with the
      * given geometry.
      *
      * @param cell: The geocell to search
      * @param returnCells: A list of of cells. This will be filled with all cells matching the criteria
      * @param g: The geometry object to consider
      * @param maxDepth: the maximum resolution to consider
      * @return  A list of geocells.
      */
    public static boolean checkCell(String cell, DataBag returnCells, Geometry g, int maxDepth) {
        // Compute bounding box for the cell
        Polygon cellBox = computeBox(cell);

        if (returnCells.size() > MAX_CELLS) {
            return false;
        }
       
        if (cellBox.intersects(g)) {

            if (cell.length() >= maxDepth ) {
                Tuple geocell = tupleFactory.newTuple(cell);
                returnCells.add(geocell);
                return true;
            } else if (g.covers(cellBox)) {
                // Optmize in this case, only go to one more depth
                maxDepth = Math.min(maxDepth, cell.length() + 1);                                    
            }
            List<String> children = childrenFor(cell);
            
            Geometry cut = g.intersection(cellBox);
            cut = (cut.getGeometryType().equals(GEOM_COLLEC) ? cut.getEnvelope() : cut );
            
            for (String child : children) {
                checkCell(child, returnCells, cut, maxDepth);
            }
        }
        return true;
    }


    /**
     * List all possible children for the given geo cell.
     *
     * @param cell: The geocell to search
     * @return  A list of children geocells.
     */
    public static List<String> childrenFor(String cell) {
        int resolution   = cell.length();
        List<String> children = new ArrayList<String>();
        StringBuilder child   = new StringBuilder(cell);
        for (int i = 0; i < GEOCELL_ALPHABET.length(); i++) {
            child.append(GEOCELL_ALPHABET.charAt(i));
            children.add(child.toString());
            child.deleteCharAt(resolution);
        }
        return children;
    }

    /**
     * Count the number of cells that would be contained within the bounding box
     * at the resolution specified
     */
    public static int cellCount(Envelope bbox, int zoom) {

        String cellNE = compute(bbox.getMaxX(), bbox.getMaxY(), zoom);
        String cellSW = compute(bbox.getMinX(), bbox.getMinY(), zoom);
        
        Polygon bboxNE = computeBox(cellNE);
        Polygon bboxSW = computeBox(cellSW);

        double cellLatSpan = bboxSW.getEnvelopeInternal().getMaxY() - bboxSW.getEnvelopeInternal().getMinY();
        double cellLonSpan = bboxSW.getEnvelopeInternal().getMaxX() - bboxSW.getEnvelopeInternal().getMinX();

        int numCols = (int)((bboxNE.getEnvelopeInternal().getMaxX() - bboxSW.getEnvelopeInternal().getMinX()) / cellLonSpan);
        int numRows = (int)((bboxNE.getEnvelopeInternal().getMaxY() - bboxSW.getEnvelopeInternal().getMinY()) / cellLatSpan);

        return numCols * numRows;
    }
    
}
