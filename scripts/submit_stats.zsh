#!/bin/zsh

function runCommand() {
    for d in ./gender*/ ; do /bin/zsh -c "(cd "$d" && "$@")"; done
}

runCommand "spark-submit \
--total-executor-cores 192 \
--executor-memory 18G \
--conf spark.executor.cores=4 \
--conf spark.driver.maxResultSize=20G \
--conf spark.sql.broadcastTimeout=1200 \
--py-files ../../dist/exploration.zip \
../../scripts/stats.py"