!#/bin/bash

echo "Submitting job1"
sbatch oc20_20m_no_dev_0_1000.sbatch &
echo "Submitting job2"
sbatch oc20_20m_no_dev_1000_2000.sbatch &
echo "Submitting job3"
sbatch oc20_20m_no_dev_2000_3000.sbatch &
echo "Submitting job4"
sbatch oc20_20m_no_dev_3000_4000.sbatch &
echo "Done"