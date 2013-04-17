REGISTER /share/pig-0.11.0/elephant-bird-2.1.2.jar;
REGISTER /share/pig-0.11.0/json-simple-1.1.1.jar;

SET  default_parallel $parallel
SET  pig.tmpfilecompression true
SET  pig.tmpfilecompression.codec lzo

/* hdfs://localhost:9000/pig/FPTI */
fptis_raw = LOAD '$input' USING com.twitter.elephantbird.pig.load.JsonLoader() as (json:map[]);
fptis = FOREACH fptis_raw GENERATE $0#'fptiTrackingCookie' as fptiTrackingCookie, $0#'ppmnTrackingCookie' as ppmnTrackingCookie;

/* hdfs://localhost:9000/pig/fpti-out */
STORE fptis INTO '$output';