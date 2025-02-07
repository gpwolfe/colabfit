# ColabFit Exchange Database Scripts
* * *
The [ColabFit Exchange](https://www.colabfit.org) is a database of collected datasets created with the intent to provide data for the training of machine-learning interatomic potentials (MLIP) or data-driven interatomic potentials (DDIP).  In short, the included data can be used to generate potentials for the prediction of properties of atomic configurations OR the generation of novel configurations.

This repository contains the scripts used to ingest data from the original files (or pre-processed files where noted) to the ColabFit database. The intent is to provide access for future trouble-shooting of data (in the event of, for example, mislabeled units or excluded configurations), as well as establish a variety of templates for ingesting other datasets, as the potential formats for original data are numerous and can be niche.  

The [`colabfit-tools`](https://github.com/colabfit/colabfit-tools) package is used in these scripts to manage reformatting and ingestion of datasets. `colabfit-tools` is still developing to meet the evolving needs of the database and its users. This development is reflected in the evolving usage in different scripts even within the [`completed_vast_ingest`](https://github.com/gpwolfe/colabfit/tree/main/completed_vast_ingest) folder, which represents scripts for ingesting datasets to the current Vast DB back end.

In 2024, the back end of ColabFit switched from MongoDB to [Vast DB](https://www.vastdata.com/), hosted on the NYU-HPC Data Lake. The MongoDB scripts have not been adapted to Vast, but exist in their original form in [`scripts_mongodb`](https://github.com/gpwolfe/colabfit/tree/main/scripts_mongodb).  
<details>
<summary> <b>Links to ColabFit and related projects.</b> </summary>
<a href="https://materials.colabfit.org">ColabFit Exchange</a>: Data for training MLIPs and DDIPs<br>
<a href="https://github.com/colabfit/colabfit-tools">colabfit-tools</a>: Python package for reformatting data into the ColabFit standard and managing a database of datasets  <br>
<a href="https://kim-initiative.org">KIM Initiative</a>: Dedicated to the development of tools for researchers in molecular dynamics simulations  <br>
<a href="https://fermat-ml.github.io/FERMat-site/">FERMat</a>: Foundation model development for molecular and materials systems and structures  <br>
<a href="https://kliff.readthedocs.io/en/latest/">KLIFF</a>: Interatomic potential-fitting package  <br>
<a href="https://openkim.org">OpenKIM</a>: Repository of interatomic potentials and analytics  <br>
<a href="https://kimreview.org">KIM REVIEW</a>: Journal with commentaries on classic and influential papers in the area of molecular simulation<br>
</details>