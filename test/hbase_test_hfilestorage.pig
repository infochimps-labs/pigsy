register 'target/infochimps-piggybank-1.0-SNAPSHOT.jar';
register '/home/jacob/Programming/troop/vendor/jars/guava-r06.jar';
register '/home/jacob/Programming/troop/vendor/jars/hbase-0.90.1-cdh3u0.jar';
register '/home/jacob/Programming/troop/vendor/jars/zookeeper-3.3.1+10.jar';

%default TABLE 'footable'
%default CF    'foocf'
%default NAMES 'fooname,aname'

--        
-- To generate test data:
--        
-- $: for foo in {1..10000}; do echo -e "$foo\t$foo"; done | hdp-put - /tmp/hbase/test/
-- 
data       = LOAD '/tmp/hbase/test_2' AS (row_key:chararray, foocolumn:chararray);
data2      = FOREACH data GENERATE row_key AS row_key, foocolumn AS foocolumn, 'a' AS acolumn;
grpd       = GROUP data2 BY row_key PARALLEL 4;
for_hfiles = FOREACH grpd GENERATE group AS row_key, data2.(foocolumn, acolumn) AS columns;
ordrd      = ORDER for_hfiles BY row_key ASC;

STORE ordrd INTO '/tmp/hbase/hfiles_test' USING com.infochimps.hadoop.pig.hbase.HFileStorage('$TABLE', '$CF', '$NAMES');
