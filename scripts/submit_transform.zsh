#!/bin/zsh

function runCommand() {
    for d in ./gender*/ ; do /bin/zsh -c "(cd "$d" && "$@")"; done
}

runCommand "spark-submit \
  --total-executor-cores 160 \
  --executor-memory 18G \
  --conf spark.executor.cores=4\
  --class fr.polytechnique.cmap.cnam.study.fall.FallMainTransform \
  --conf spark.driver.maxResultSize=20G \
  --conf spark.sql.broadcastTimeout=1200 \
  --conf spark.locality.wait=5s \
  --conf spark.eventLog.enabled=false \
  ../../dist/extraction.jar env=cnam conf=../../dist/fall.conf meta_bin=../../dist/meta.bin"