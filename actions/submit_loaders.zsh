#!/bin/zsh

function runCommand() {
    for d in ./G*/ ; do /bin/zsh -c "(cd "$d" && "$@")"; done
}

runCommand "spark-submit \
--total-executor-cores 192 \
--executor-memory 32G \
--conf spark.executor.cores=4 \
--conf spark.task.maxFailures=20 \
--conf spark.driver.maxResultSize=20G \
--conf spark.sql.broadcastTimeout=1200 \
--conf spark.locality.wait=5s \
--conf spark.eventLog.enabled=false \
--conf spark.sql.parquet.compression.codec='uncompressed' \
--py-files ../../dist/exploration.zip,../../dist/parameters.zip \
../../scripts/load_convsscs.py"