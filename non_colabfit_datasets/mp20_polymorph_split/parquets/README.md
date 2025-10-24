---
configs:
- config_name: default
  data_files: 
  - split: train
    path: "train.parquet"
  - split: val
    path: "val.parquet"
  - split: test
    path: "test.parquet"

license: cc-by-4.0
tags:
  - generative modeling
  - materials discovery
  - DFT
pretty_name: Alex-MP-20 Polymorph Split
---
### Dataset Name  
Alex-MP-20 Polymorph Split   
### Citation  
Please cite [Martirossyan et al. (https://arxiv.org/abs/2509.12178)](https://arxiv.org/abs/2509.12178) if your work utilizes this dataset.  
### Description  
A new split for performing crystal structure prediction on the [Alex-MP-20 dataset (https://github.com/microsoft/mattergen/tree/main/data-release/alex-mp)](https://github.com/microsoft/mattergen/tree/main/data-release/alex-mp) which contains structures from MP-20 (Jain 2013, doi: 10.1063/1.4812323) and Alexandria (Schmidt 2022, doi: 10.24435/materialscloud:m7-50).  
<br>
This dataset ensures that structures of the same composition remain together in the same split.  
### License  
CC-BY-4.0  
