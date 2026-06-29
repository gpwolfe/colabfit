#!/bin/bash

#SBATCH --job-name=shortname
#SBATCH --mail-type=BEGIN,END
#SBATCH --mail-user=
#SBATCH --nodes=1 # only use 1 node 
#SBATCH --account=
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
    python3 script_name.py
    "


