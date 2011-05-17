--
-- It's not a real test yet. You'll have to modify the paths to jars below.
--
-- USAGE:
--
-- pig -x local hbase_test_dynamicfamilystorage.pig
--
--
register '/home/jacob/Programming/troop/vendor/jars/guava-r06.jar';
register '/home/jacob/Programming/troop/vendor/jars/hbase-0.90.1-cdh3u0.jar';
register '/home/jacob/Programming/troop/vendor/jars/zookeeper-3.3.1+10.jar';
register '../target/infochimps-piggybank-1.0-SNAPSHOT.jar';

--
-- Test writes
--
write_data = LOAD 'data/some_screen_names_dynfamstorage.tsv' AS (row_key:chararray, column_family:chararray, column_name:chararray, column_value:chararray);
STORE write_data INTO 'chh' USING com.infochimps.hadoop.pig.hbase.DynamicFamilyStorage('/etc/hbase/conf/hbase-site.xml');
