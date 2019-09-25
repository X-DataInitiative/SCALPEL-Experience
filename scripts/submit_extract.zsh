#!/bin/zsh

spark-submit \
  --total-executor-cores 160 \
  --executor-memory 18G \
  --class fr.polytechnique.cmap.cnam.study.fall.FallMainExtract \
  --conf spark.scheduler.listenerbus.eventqueue.capacity=20000 \
  --conf spark.sql.shuffle.partitions=200\
  --conf spark.executor.cores=4\
  --conf spark.default.parallelism=200\
  --conf spark.task.maxFailures=20 \
  --conf spark.driver.maxResultSize=20G \
  --conf spark.sql.broadcastTimeout=1200 \
  --conf spark.locality.wait=5s \
  --conf spark.eventLog.enabled=false \
  --conf spark.sql.parquet.compression.codec='uncompressed' \
  ../dist/extraction.jar env=cnam conf=../dist/fall.conf meta_bin=../dist/meta.bin
