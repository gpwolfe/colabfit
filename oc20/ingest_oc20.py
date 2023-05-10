from argparse import ArgumentParser
from colabfit.tools.database import MongoDatabase, load_data
from colabfit.tools.property_definitions import (
    potential_energy_pd,
    atomic_forces_pd,
    free_energy_pd,
)
from pathlib import Path
import sys

DATASET_FP = Path("/persistent/colabfit_raw_data/oc20")
AUTHORS = [
    "Lowik Chanussot",
    "Abhishek Das",
    "Siddharth Goyal",
    "Thibaut Lavril",
    "Muhammed Shuaibi",
    "Morgane Riviere",
    "Kevin Tran",
    "Javier Heras-Domingo",
    "Caleb Ho",
    "Weihua Hu",
    "Aini Palizhati",
    "Anuroop Sriram",
    "Brandon Wood",
    "Junwoong Yoon",
    "Devi Parikh",
    "C. Lawrence Zitnick",
    "Zachary Ulissi",
]
LINKS = [
    "https://arxiv.org/abs/2010.09990",
    "https://github.com/Open-Catalyst-Project/ocp/blob/"
    "main/DATASET.md#structure-to-energy-and-forces-s2ef-task",
]
DATASET = "OC20_S2EF_Train_All"
DS_DESC = "All configurations from the OC20 S2EF training set"

# def tform(c):
#     print(c.info["miller_index"])
#     print(c.info["adsorption_site"])
#     c.info["miller_index"] = c.info["miller_index"].remove("_JSON ")
#     c.info["adsorption_site"] = c.info["adsorption_site"].remove("_JSON ")


def main(argv):
    parser = ArgumentParser()
    parser.add_argument("-i", "--ip", type=str, help="IP of host mongod")
    parser.add_argument(
        "-d",
        "--db_name",
        type=str,
        help="Name of MongoDB database to add dataset to",
        default="----",
    )
    parser.add_argument(
        "-p",
        "--nprocs",
        type=int,
        help="Number of processors to use for job",
        default=4,
    )
    args = parser.parse_args(argv)
    client = MongoDatabase(
        args.db_name, nprocs=args.nprocs, uri=f"mongodb://{args.ip}:27017"
    )
    client.insert_property_definition(potential_energy_pd)
    client.insert_property_definition(free_energy_pd)
    client.insert_property_definition(atomic_forces_pd)

    # bulk_id = f.split("/")[-1]
    configurations = list(
        load_data(
            file_path=DATASET_FP,
            file_format="xyz",
            glob_string="*.xyz",
            name_field="oc-name",
            elements=None,
            # default_name=name,
            verbose=True,
        )
    )

    property_map = {
        "potential-energy": [
            {
                "energy": {"field": "energy", "units": "eV"},
                "per-atom": {"value": False, "units": None},
                "_metadata": {
                    "software": {"value": "VASP"},
                    "method": {"value": "DFT-PBE"},
                    "reference_energy": {"field": "ref_energy"},
                },
            }
        ],
        "atomic-forces": [
            {
                "forces": {"field": "forces", "units": "eV/Ang"},
                "_metadata": {
                    "software": {"value": "VASP"},
                    "method": {"value": "DFT-PBE"},
                    "reference_energy": {"field": "ref_energy"},
                },
            }
        ],
        "free-energy": [
            {
                "energy": {"field": "free_energy", "units": "eV"},
                "_metadata": {
                    "software": {"value": "VASP"},
                    "method": {"value": "DFT-PBE"},
                },
            }
        ],
    }

    ids = list(
        client.insert_data(
            configurations,
            property_map=property_map,
            co_md_map={
                "bulk_id": {"field": "bulk_id"},
                "ads_id": {"field": "ads_id"},
                "bulk_symbols": {"field": "bulk_symbols"},
                "ads_symbols": {"field": "ads_symbols"},
                "miller_index": {"field": "miller_index"},
                "shift": {"field": "shift"},
                "adsorption_site": {"field": "adsorption_site"},
                "oc_class": {"field": "class"},
                "oc_anomaly": {"field": "anomaly"},
                "frame": {"field": "frame"},
            },
            generator=False,
            # transform=tform,
            verbose=True,
        )
    )

    all_co_ids, all_pr_ids = list(zip(*ids))

    # nm = sorted(glob("%s/*.xyz" % f))
    # configuration_set_regexes = {
    #     "%s"
    #     % it.split("/")[-1].split(".")[0]: "OC20 IS2RE training trajectory for %s."
    #     % it.split("/")[-1].split(".")[0]
    #     for it in nm
    # }

    # cs_name = ["IS2RE_%s" % it.split("/")[-1].split(".")[0] for it in nm]

    # print(cs_name)

    # cs_ids = []

    # for i, (regex, desc) in enumerate(configuration_set_regexes.items()):
    #     co_ids = client.get_data(
    #         "configurations",
    #         fields="hash",
    #         query={"hash": {"$in": all_co_ids}, "names": {"$regex": regex}},
    #         ravel=True,
    #     ).tolist()

    #     print(
    #         f"Configuration set {i}", f"({regex}):".rjust(22), f"{len(co_ids)}".rjust(7)
    #     )

    #     cs_id = client.insert_configuration_set(
    #         co_ids, description=desc, name=cs_name[i]
    #     )

    #     cs_ids.append(cs_id)

    client.insert_dataset(
        do_hashes=all_pr_ids,
        name=DATASET,
        authors=AUTHORS,
        links=LINKS,
        description=DS_DESC,
        verbose=True,
    )


if __name__ == "__main__":
    args = sys.argv[1:]
    main(args)

    # parser = get_parser()
    # args = parser.parse_args()
    # jobarray = args.jobarray
    # ip = args.ip

    # files = sorted(
    #     glob("/scratch/work/martiniani/is2res_train_trajectories/is_sorted/*")
    # )
    # ingest(jobarray, f)
