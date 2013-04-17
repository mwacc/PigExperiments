REGISTER /share/pig-0.11.0/elephant-bird-2.1.2.jar;
REGISTER /share/pig-0.11.0/json-simple-1.1.1.jar;

SET  default_parallel $parallel
SET  pig.tmpfilecompression true
SET  pig.tmpfilecompression.codec lzo

fptis = LOAD 'hdfs://localhost:9000/pig/fpti-out' AS (fptiTrackingCookie:chararray, ppmnTrackingCookie:chararray);
bml_xos = LOAD 'hdfs://localhost:9000/pig/xo-out' AS (ip:chararray, fptiTrackingCookie:chararray, encryptedMechantId:chararray, encryptedCustomerId:chararray, ts:chararray, fundingSource:chararray);

bml = JOIN fptis BY fptiTrackingCookie, bml_xos BY fptiTrackingCookie;