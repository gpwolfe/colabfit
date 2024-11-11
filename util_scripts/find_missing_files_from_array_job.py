import pyspark.sql.functions as sf
from pyspark.sql.types import IntegerType
from pyspark.sql import SparkSession

# from argparse import ArgumentParser

FILE_NOS = list(range(0, 26787))


# def main(beginning_ixs, step_size=1000):
def main():
    spark = SparkSession.builder.appName("test").getOrCreate()
    cos = spark.table("ndb.colabfit.dev.co_oc_reset")

    name_udf = sf.udf(
        lambda x: int(x.split("OC20_S2EF_train_all__file_")[1].split("__")[0]),
        IntegerType(),
    )
    cos = cos.filter('names like "%OC20_S2EF_train_all__file_%"')
    cos = (
        cos.withColumn("file_no", name_udf(sf.col("names")))
        .select("file_no")
        .dropDuplicates(["file_no"])
    )
    # for ix in beginning_ixs:
    #     cos_nums = cos.filter(
    #         (sf.col("file_no") >= ix) & (sf.col("file_no") < (ix + step_size))
    #     ).distinct()
    cos_nums = [int(x["file_no"]) for x in cos.collect()]
    print(len(cos_nums))
    # print(sorted(cos_nums))
    # print(type(cos_nums[0]))
    missing_nums = []
    # print(ix, ix + step_size)
    for i in FILE_NOS:
        if i not in cos_nums:
            print(i)
            missing_nums.append(i)
    print(missing_nums)
    with open(f"oc20_train_all_missing_files_0_{max(FILE_NOS)}_11_11_24.txt", "w") as f:
        f.write(str(missing_nums)[1:-1].replace(" ", ""))


if __name__ == "__main__":
    # argpar = ArgumentParser()
    # argpar.add_argument("--beginning_ixs", nargs="+", type=int)
    # argpar.add_argument("--step_size", type=int, default=1000)
    # args = argpar.parse_args()
    # beginning_ixs = args.beginning_ixs
    # step_size = args.step_size
    main()
