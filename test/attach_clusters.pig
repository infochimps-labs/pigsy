register 'target/infochimps-piggybank-1.1.0-SNAPSHOT.jar';
register 'lib/json-20080701.jar';
register 'lib/jts-1.8.jar';
register 'lib/mapfish-geo-lib-1.3-SNAPSHOT.jar';

--
-- 1. Read in cluster polygons and points
-- 2. Tile cluster polygons and points at the same zoom level
-- 3. Cogroup by quadkey
-- 4. For every point in the points bag determine the set of cluster polygons
--    it lies inside
-- 5. Serialize out (cluster_id, cluster_polygon, point)
-- 6. Group by cluster_id
-- 7. Take the bag of points that map to a cluster and the cluster polygon itself and
--    construct a new 'cluster' object with an md5id, etc.
-- 8. Examine the domain id of the cluster to determine the set of zoom levels to map it to
--    and break it into tiles at those zoom levels
-- 9. Store the results into HBase as centroids
--

-- init_clusters = LOAD '/tmp/geoadventure/4sq_city_clusters' AS (json:chararray);
-- keys_1  = FOREACH init_clusters    GENERATE FLATTEN(com.infochimps.hadoop.pig.geo.SearchTile('', json))      AS (quadkey:chararray, json:chararray); -- everything at ZL=1
-- keys_2  = FOREACH keys_1  GENERATE FLATTEN(com.infochimps.hadoop.pig.geo.SearchTile(quadkey, json)) AS (quadkey:chararray, json:chararray); -- ZL=2
-- keys_3  = FOREACH keys_2  GENERATE FLATTEN(com.infochimps.hadoop.pig.geo.SearchTile(quadkey, json)) AS (quadkey:chararray, json:chararray);
-- keys_4  = FOREACH keys_3  GENERATE FLATTEN(com.infochimps.hadoop.pig.geo.SearchTile(quadkey, json)) AS (quadkey:chararray, json:chararray);
-- keys_5  = FOREACH keys_4  GENERATE FLATTEN(com.infochimps.hadoop.pig.geo.SearchTile(quadkey, json)) AS (quadkey:chararray, json:chararray);
-- keys_6  = FOREACH keys_5  GENERATE FLATTEN(com.infochimps.hadoop.pig.geo.SearchTile(quadkey, json)) AS (quadkey:chararray, json:chararray);
-- keys_7  = FOREACH keys_6  GENERATE FLATTEN(com.infochimps.hadoop.pig.geo.SearchTile(quadkey, json)) AS (quadkey:chararray, json:chararray);
-- keys_8  = FOREACH keys_7  GENERATE FLATTEN(com.infochimps.hadoop.pig.geo.SearchTile(quadkey, json)) AS (quadkey:chararray, json:chararray); -- ZL=8
-- keys_9  = FOREACH keys_8  GENERATE FLATTEN(com.infochimps.hadoop.pig.geo.SearchTile(quadkey, json)) AS (quadkey:chararray, json:chararray);
-- keys_10 = FOREACH keys_9  GENERATE FLATTEN(com.infochimps.hadoop.pig.geo.SearchTile(quadkey, json)) AS (quadkey:chararray, json:chararray);
-- keys_11 = FOREACH keys_10 GENERATE FLATTEN(com.infochimps.hadoop.pig.geo.SearchTile(quadkey, json)) AS (quadkey:chararray, json:chararray); -- ZL=11
-- 
-- --
-- -- These should go into an s3 bucket
-- --
-- STORE keys_11 INTO '/tmp/geoadventure/4sq_city_clusters_tiled_ZL11';
-- keys_11 = LOAD '/tmp/geoadventure/4sq_city_clusters_tiled_ZL11' AS (quadkey:chararray, json:chararray);
-- schools      = LOAD '/tmp/geoadventure/school.geojson' AS (json:chararray);
-- schools_ZL11 = FOREACH schools GENERATE FLATTEN(com.infochimps.hadoop.pig.geo.FeatureToQuadKeys(11, json)) AS (quadkey:chararray), json AS json;
-- -- 
-- to_clusters = FOREACH (COGROUP schools_ZL11 BY quadkey, keys_11 BY quadkey) GENERATE FLATTEN(com.infochimps.hadoop.pig.geo.PointsToClusters(schools_ZL11.json, keys_11.json)) AS (cluster_id:chararray, cluster_domain_id:chararray, point_json:chararray);
-- STORE to_clusters INTO '/tmp/geoadventure/geonames_school_clusters_test';
to_clusters = LOAD '/tmp/geoadventure/geonames_school_clusters_test' AS (cluster_id:chararray, cluster_domain_id:chararray, point_json:chararray);
oops = FOREACH to_clusters GENERATE cluster_id AS cluster_id, cluster_domain_id AS cluster_domain_id, com.infochimps.hadoop.pig.geo.AttachGUID('geo.location.geonames', 'geonameid', point_json) AS point_json;
grouped = FOREACH (GROUP oops BY cluster_id) GENERATE group AS cluster_id, MAX(oops.cluster_domain_id) AS cluster_domain_id, oops.point_json AS bag_of_points; 
new_clusters = FOREACH grouped GENERATE FLATTEN(com.infochimps.hadoop.pig.geo.CreateClusters('geo.location.geonames.school', cluster_domain_id, bag_of_points)) AS (quadkey:chararray, cluster:chararray);
STORE new_clusters INTO '/tmp/geoadventure/geonames_schools_clustered';
