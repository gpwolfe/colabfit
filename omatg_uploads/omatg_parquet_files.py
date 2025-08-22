# from huggingface_hub import HfApi, delete_repo
from pathlib import Path

# import os
from dotenv import load_dotenv
import lmdb
import pickle as pkl
import pyarrow.parquet as pq
import pyarrow as pa
from types import GeneratorType
from itertools import islice

load_dotenv()
# {'space_group': 'R-3m',
#  'chemical_system': 'Mg-Tb-Zn',
#  'energy_above_hull': 0.0156084848103585,
# 'dft_band_gap': 'nan',
# 'dft_bulk_modulus': 'nan',
# 'dft_mag_density': 1.1327067478853874e-06,
# 'hhi_score': 2132.5377262631514,
#     'ml_bulk_modulus':0
# }
schema = pa.schema(
    [
        pa.field("pos", pa.list_(pa.list_(pa.float64()))),
        pa.field("cell", pa.list_(pa.list_(pa.float64()))),
        pa.field("atomic_numbers", pa.list_(pa.int32())),
        pa.field("pbc", pa.list_(pa.int32())),
        pa.field("ids", pa.string()),
        pa.field("space_group", pa.string()),
        pa.field("chemical_system", pa.string()),
        pa.field("energy_above_hull", pa.float64()),
        pa.field("dft_band_gap", pa.float64()),
        pa.field("dft_bulk_modulus", pa.float64()),
        pa.field("dft_mag_density", pa.float64()),
        pa.field("hhi_score", pa.float64()),
        pa.field("ml_bulk_modulus", pa.float64()),
    ]
)


def batched(configs, n):
    if not isinstance(configs, GeneratorType):
        configs = iter(configs)
    while True:
        batch = list(islice(configs, n))
        if len(batch) == 0:
            break
        yield batch


def process_rows_generator(file):
    env = lmdb.open(str(file), readonly=True, lock=False, subdir=False)
    txn = env.begin(write=False)
    cursor = txn.cursor()
    for key, row in cursor:
        row = pkl.loads(row)
        processed_row = {}
        processed_row["pos"] = row["pos"].detach().cpu().numpy().tolist()
        processed_row["cell"] = row["cell"].detach().cpu().numpy().tolist()
        processed_row["atomic_numbers"] = (
            row["atomic_numbers"].detach().cpu().numpy().tolist()
        )
        processed_row["pbc"] = row["pbc"].detach().cpu().numpy().tolist()
        processed_row["dft_band_gap"] = float(row["dft_band_gap"])
        processed_row["dft_bulk_modulus"] = float(row["dft_bulk_modulus"])
        processed_row["dft_mag_density"] = float(row["dft_mag_density"])
        processed_row["hhi_score"] = float(row["hhi_score"])
        processed_row["ml_bulk_modulus"] = float(row["ml_bulk_modulus"])
        processed_row["space_group"] = row["space_group"]
        processed_row["chemical_system"] = row["chemical_system"]
        processed_row["ids"] = row["ids"]
        yield processed_row


def main():
    lmdb_files = sorted(list(Path("/scratch/ph2484/shared/alex_mp_20").glob("*.lmdb")))
    print(lmdb_files)
    for file in lmdb_files:
        print(f"Processing {file}")
        output_dir = Path(f"alex_mp_20_{file.stem}")
        output_dir.mkdir(exist_ok=True)
        gen = process_rows_generator(file)
        for i, batch in enumerate(batched(gen, 5000)):
            table = pa.Table.from_pylist(batch, schema=schema)
            output_file = output_dir / f"alex_mp_20_{file.stem}__{i}.parquet"
            pq.write_table(table, output_file)
            print(f"Wrote {output_file}")


if __name__ == "__main__":
    main()
