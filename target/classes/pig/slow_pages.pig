SET  default_parallel $parallel
SET  mapred.job.name 'Average response time by URL'

raw_log = LOAD '$input' using PigStorage(',');
statistics = FOREACH raw_log GENERATE $2 as url, $4 as res_time;

by_url = group statistics by url;

grouped = FOREACH by_url GENERATE
    group as url,
    AVG(statistics.res_time);

STORE grouped INTO '$output';