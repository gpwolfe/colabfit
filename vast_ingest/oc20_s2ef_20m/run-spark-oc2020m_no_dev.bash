#!/bin/bash

source /scratch/work/public/apps/pyspark/3.4.2/scripts/spark-setup-slurm.bash
echo "run-spark-mtpu.bash"
start_all
printenv
spark-submit \
--master=${SPARK_URL} \
--executor-memory=${MEMORY} \
--driver-memory=${MEMORY} \
--jars=${VASTDB_CONNECTOR_JARS} \
--py-files="/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/oc20_s2ef/oc20_s2ef_20m.py" \
/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/oc20_s2ef/oc20_s2ef_20m.py

stop_all
