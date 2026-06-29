#!/bin/bash

#SBATCH --job-name=nabladft2k
#SBATCH --mail-type=BEGIN,END
#SBATCH --mail-user=gw2338@nyu.edu # cannot be changed
#SBATCH --nodes=1 # only use 1 node
#SBATCH --account=torch_pr_148_general # cannot be changed
#SBATCH --cpus-per-task=2
#SBATCH --output=logs_%A.out
#SBATCH --mem=80GB
#SBATCH --time=4:00:00


export CUSTOM_CONDA_ENV="/scratch/gw2338/on_demand_overlay.ext3"
singularity exec --overlay $CUSTOM_CONDA_ENV:ro \
/share/apps/images/ubuntu-22.04.4.sif \
    /bin/bash -c "
    unset -f which
    source /ext3/env.sh
    python3 nabladft_test_2k_conformers.py
    "
