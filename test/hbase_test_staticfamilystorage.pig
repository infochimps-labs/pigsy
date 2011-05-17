--
-- It's not a real test yet. You'll have to modify the paths to jars below.
--
-- USAGE:
--
-- pig -x local hbase_test_staticfamilystorage.pig
--
--
register '/home/jacob/Programming/troop/vendor/jars/guava-r06.jar';
register '/home/jacob/Programming/troop/vendor/jars/hbase-0.90.1-cdh3u0.jar';
register '/home/jacob/Programming/troop/vendor/jars/zookeeper-3.3.1+10.jar';
register '../target/infochimps-piggybank-1.0-SNAPSHOT.jar';

--
-- Test reads
--
read_data = LOAD 'chh' USING com.infochimps.hadoop.pig.hbase.StaticFamilyStorage('screen_name:screen_name', '-loadKey -limit 10 -config /etc/hbase/conf/hbase-site.xml');
DUMP read_data;

--
-- Test writes
--
write_data = LOAD 'data/some_screen_names.tsv' AS (row_key:chararray, screen_name:chararray);
STORE write_data INTO 'chh' USING com.infochimps.hadoop.pig.hbase.StaticFamilyStorage('screen_name:screen_name', '-config /etc/hbase/conf/hbase-site.xml');
