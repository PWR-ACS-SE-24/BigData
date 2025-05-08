#!/bin/env bash
docker exec -it master bash -c "beeline -u 'jdbc:hive2://localhost:10000 hive org.apache.hive.jdbc.HiveDriver' --hiveconf hive.stats.autogather=false"
