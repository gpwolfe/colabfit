# colabfit
Files related to the ColabFit project, including scripts for ingesting data.

* `scripts` contains scripts that have been ingested into at least one of the ColabFit test databases.  
* `scripts_13_7_2023` contains in-progress scripts and those which have not yet been ingested into the ColabFit database.  
* `scripts_large` contains scripts with datasets large enough to require special attention when ingesting. These may need to be run from the Greene HPC for additional memory and processing resources. See notes in each script for additional details.  

Scripts are currently written to be run either alone or in batches of any size using a separate run script.  
Dependencies for these scripts combined are listed in requirements.txt. This is not currently a minimal dependencies list.  

An example/template script is provided (`template.py`)  
