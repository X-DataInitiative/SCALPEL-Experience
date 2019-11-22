#!/bin/zsh

function runCommand() {
    for d in ./gender*/ ; do /bin/zsh -c "(cd "$d" && "$@")"; done
}

runCommand "spark-submit \
--total-executor-cores 160 \
--executor-memory 28G \
--conf spark.executor.cores=4 \
--conf spark.driver.memory=28g \
--conf spark.driver.maxResultSize=20G \
--conf spark.sql.broadcastTimeout=1200 \
--py-files ../../dist/exploration.zip,../../dist/parameters.zip \
../../scripts/stats.py"