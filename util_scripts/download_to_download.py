"""Driver: download every file (data + readme) for each Zenodo record listed in
vast_ingest/to_download.md into a descriptively named dataset directory.

The dataset directory names are derived from each record's Zenodo title.
File lists are pulled live from the Zenodo API so nothing is transcribed by hand.
"""

import json
import urllib.request
from urllib.parse import quote

from download_data import download

BROWSER_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json",
}

# Zenodo record id -> dataset directory name (chosen from each record's title).
RECORDS = {
    "18868464": "hcb_montmorillonite_adsorption",
    "18260745": "perovskite_nanorod_li_na_transport",
    "19814862": "metal_phi_carbon_nitride_catalysis",
    "20033455": "ceo2_surface_oxygen_vacancy_mlff",
    "17930461": "copt_dry_reforming_methane",
    "19849722": "lani5_lasn5_hydride_vasp",
    "19951919": "halide_perovskite_ion_migration_mlff",
    "18846704": "la_zrs2_water_splitting",
    "18841767": "halide_double_perovskite_octahedral_tilting",
    "18471952": "metalloporphyrin_nanotape_magnetism",
    "19236518": "graphene_fet_urea_sensor",
    "20053085": "silica_allegro_15000k",
    "18863304": "mnte_anomalous_hall_afm",
}


def file_keys(record_id):
    url = f"https://zenodo.org/api/records/{record_id}"
    req = urllib.request.Request(url, headers=BROWSER_HEADERS)
    data = json.load(urllib.request.urlopen(req, timeout=60))
    return [f["key"] for f in data.get("files", [])]


def main():
    for record_id, dataset_dir in RECORDS.items():
        print(f"\n===== {record_id} -> {dataset_dir} =====")
        for key in file_keys(record_id):
            file_url = f"https://zenodo.org/records/{record_id}/files/{quote(key)}?download=1"
            download(file_url, output_dir=dataset_dir)


if __name__ == "__main__":
    main()
