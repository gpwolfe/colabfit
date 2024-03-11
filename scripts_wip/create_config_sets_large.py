import json
from argparse import ArgumentParser
from pathlib import Path

from colabfit.tools.database import MongoDatabase


def insert_cs(co_id_fp, cs_id_fp, ds_id, ds_name, ip, port, db_name, nprocs):
    try:
        natoms = int(co_id_fp.stem.split("_")[-1])
        with open(co_id_fp, "r") as f:
            co_ids = [line.strip().replace("CO_", "") for line in f.readlines()]
            print(f"number of COs in natoms batch {natoms}: {len(co_ids)}")
        client = MongoDatabase(
            database_name=db_name, uri=f"mongodb://{ip}:{port}", nprocs=nprocs
        )

        name = f"{ds_name}_num_atoms_{natoms}"
        print(f"CS name: {name}")
        desc = f"Configurations with {natoms} atoms from {ds_name} dataset"

        cs_id = client.query_and_insert_configuration_set(
            co_hashes=co_ids,
            ds_id=ds_id,
            name=name,
            description=desc,
            query=None,
        )
        with open(cs_id_fp, "a") as f:
            f.write(f"{cs_id}\n")
        client.close()
        return cs_id
    except Exception as e:
        with open(f"{ds_id}_error_log.txt", "a") as f:
            f.write(f"{ds_id} {co_id_fp} {e}\n")
    finally:
        client.close()


def main(args):

    nprocs = args.p
    db_name = args.db
    ip = args.i
    port = args.r
    ds_data = json.load(args.ds_data.open("r"))
    cs_ids_fp = Path(ds_data["cs_ids_fp"])
    co_ids_dir = Path(ds_data["co_ids_dir"])
    ds_id = ds_data["dataset_id"]
    ds_name = ds_data["dataset_name"]

    fps = sorted(list(co_ids_dir.rglob(f"{ds_id}*.txt")), key=lambda x: x.stem)
    for fp in fps:
        print(fp)
        insert_cs(
            co_id_fp=fp,
            cs_id_fp=cs_ids_fp,
            ds_id=ds_id,
            ds_name=ds_name,
            ip=ip,
            port=port,
            db_name=db_name,
            nprocs=nprocs,
        )


if __name__ == "__main__":
    parser = ArgumentParser()
    parser.add_argument("-p", type=int, default=16)
    parser.add_argument("-d", "--db", type=str, help="Name of database", required=True)
    parser.add_argument("-i", type=str, help="IP/host for MongoDB", required=True)
    parser.add_argument("-r", type=int, help="port for MongoDB", required=True)
    parser.add_argument("-f", "--ds_data", type=Path, help="File of dataset data")
    args = parser.parse_args()
    main(args)
