RedisParse
==========

Reads huge amounts of pipe-delimited data from Redis, filters it, and returns counts by occurence.

Usage
-----

    Usage:
        redis_parse IP PORT 'REDIS QUERY' COUNT|GROUP START END LIMIT COLUMN 'QUERY'

        IP              IP address of redis server
        PORT            Port of redis server
        REDIS QUERY     http://redis.io/commands
        COUNT|GROUP     COUNT if only returning integer (number of matches),
                        GROUP if returning column and number of matches separated by comma
        START           Only works if WITHSCORES present in redis query, start range for score
        FINISH          Only works if WITHSCORES present in redis query, end range for score
        LIMIT           Only works if type GROUP, limit results
        COLUMN          Only works if type GROUP - referrer, time, tags, article_id, keywords, or type
        QUERY           Pipe delimited query string representing matches on the columns:
                        referrer|time|tags|article_id|keywords|type
        EXCLUDE         Pipe delimited query string representing matches to exclude on the columns:
                        referrer|time|tags|article_id|keywords|type

    Example:
        redis_parse 127.0.0.1 6379 'ZRANGEBYSCORE hits -inf +inf' COUNT -1 -1 100 referrer 'a|b|c|d|e|f' 'a|b|c|d|e|f'