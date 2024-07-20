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
--py-files="/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/comp6v2/comp6v2_wb97md3bj_def2tzvpp.py" \
/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/comp6v2/comp6v2_wb97md3bj_def2tzvpp.py

stop_all
