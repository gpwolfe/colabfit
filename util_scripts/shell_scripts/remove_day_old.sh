#!/bin/bash


find /vast/gw2338/spark-tmp -type d -mtime +1 -exec rm -rf {} +
find /scratch/gw2338/spark-tmp -maxdepth 2 -type d -mtime +1 -exec rm -rf {} +

# find "$TARGET_DIR" -type f -name "slurm*" -exec rm -r {} \;