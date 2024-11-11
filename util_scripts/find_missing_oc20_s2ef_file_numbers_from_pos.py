import pyspark.sql.functions as sf
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType


def main():
    spark = SparkSession.builder.appName("test").getOrCreate()
    cos = spark.table("ndb.colabfit.dev.co_oc_reset")
    pos = spark.table("ndb.colabfit.dev.po_oc_reset")
    pos = (
        pos.filter('dataset_id = "DS_jyuwhl30jklq_0"')
        .select("configuration_id")
        .distinct()
    )
    name_udf = sf.udf(
        lambda x: int(x.split("OC20_S2EF_train_all__file_")[1].split("__")[0]),
        IntegerType(),
    )
    cos = cos.filter('names like "%OC20_S2EF_train_all__file_%"')
    cos = cos.withColumn("file_no", name_udf(sf.col("names"))).select("file_no", "id")
    cos = cos.dropDuplicates(["file_no"])
    cos = cos.join(pos, cos.id == pos.configuration_id, "left_anti")
    file_nums = sorted([x["file_no"] for x in cos.collect()])
    print(file_nums)
    with open("oc20_s2ef_missing_file_nums_join.txt", "w") as f:
        f.write(str(file_nums).replace(" ", ""))


if __name__ == "__main__":
    main()
