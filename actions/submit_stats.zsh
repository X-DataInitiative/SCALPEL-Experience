#!/bin/zsh

function runCommand() {
    for d in ./G*/ ; do /bin/zsh -c "(cd "$d" && "$@")"; done
}

runCommand "spark-submit \
--total-executor-cores 160 \
--executor-memory 32G \
--conf spark.executor.cores=4 \
--conf spark.driver.maxResultSize=20G \
--conf spark.sql.broadcastTimeout=1200 \
--py-files ../../dist/scalpel.zip,../../dist/parameters.zip \
../../scripts/stats.py"