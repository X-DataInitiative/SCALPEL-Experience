#!/bin/zsh

function runCommand() {
    for d in ./gender*/ ; do /bin/zsh -c "(cd "$d" && "$@")"; done
}

runCommand "spark-submit \
  --total-executor-cores 192 \
  --executor-memory 18G \
  --conf spark.executor.cores=4\
  --class fr.polytechnique.cmap.cnam.study.fall.FallMainTransform \
  --conf spark.driver.maxResultSize=20G \
  --conf spark.sql.broadcastTimeout=1200 \
  --conf spark.locality.wait=5s \
  --conf spark.eventLog.enabled=false \
  /home/sebiat/builds/master/SNIIRAM-featuring-assembly-2.0-95b9b54109bfa64b1ae2ebce33c716a5f39cc6ed-SNAPSHOT.jar env=cmap conf=fall.conf meta_bin=/home/sebiat/builds/master/meta.bin"