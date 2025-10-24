---
configs:
- config_name: default
  data_files: 
  - split: train
    path: "train.parquet"
  - split: val
    path: "val.parquet"

license: cc-by-4.0
tags:
  - generative modeling
  - materials discovery
  - DFT
pretty_name: carbon-enantiomorphs
---

### Dataset  Name  
carbon-enantiomorphs     
### Citation  
Please cite [Martirossyan et al. (https://arxiv.org/abs/2509.12178)](https://arxiv.org/abs/2509.12178) if your work utilizes this dataset.  
### Description  
This carbon-enantiomorphs dataset is cultivated from the [carbon-24-unique-with-enantiomorphs dataset](https://huggingface.co/datasets/colabfit/carbon-24_unique_with_enantiomorphs).  
<br>
<b>Contents:</b>  
train.xyz - 80 structures which are chiral  
test.xyz - 80 of the same structures with opposite handedness  

### License  
CC-BY-4.0  

