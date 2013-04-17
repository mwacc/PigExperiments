REGISTER /share/pig-0.11.0/elephant-bird-2.1.2.jar;
REGISTER /share/pig-0.11.0/json-simple-1.1.1.jar;

SET  default_parallel $parallel
SET  pig.tmpfilecompression true
SET  pig.tmpfilecompression.codec lzo

/* hdfs://localhost:9000/pig/XO */
xos_raw = LOAD '$input' USING com.twitter.elephantbird.pig.load.JsonLoader() as (json:map[]);

xos = FOREACH xos_raw GENERATE $0#'ip' as ip, $0#'fptiTrackingCookie' as fptiTrackingCookie, $0#'EncryptedMechantId' as encryptedMechantId,
 $0#'EncryptedCustomerId' as encryptedCustomerId, $0#'ts' as ts, $0#'fundingSource' as fundingSource;

bml_xos = FILTER xos BY (fundingSource == 'BML');

/* hdfs://localhost:9000/pig/xo-out */
STORE bml_xos INTO '$output';