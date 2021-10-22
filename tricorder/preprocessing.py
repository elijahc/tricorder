from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, collect_list, collect_set
from pyspark.sql.types import StructType, StringType, LongType, IntegerType, FloatType

def preprocess_flowsheet(data_source, output_path):

    with SparkSession.builder.appName("preprocess flowsheet").getOrCreate() as spark:
        if data_source is not None:

            schema = StructType() \
                    .add("encounter_id",LongType(),nullable=False) \
                    .add("flowsheet_days_since_birth",IntegerType(),nullable=False) \
                    .add("flowsheet_time",StringType(),True) \
                    .add("display_name",StringType(),True) \
                    .add("flowsheet_value",StringType(),True)

            f_df = spark.read.format('csv') \
                    .option('header',True) \
                    .schema(schema) \
                    .load(data_source)

            # df2 = f_df.withColumn('encounter_id',f_df.encounter_id.cast('int'))
            df3 = f_df.dropna(subset=('encounter_id','flowsheet_days_since_birth'))
            # f_df.select(collect_set('encounter_id')).show(truncate=False)
            df3.printSchema()
            df3.show()

            # df2.printSchema()
            # df2.show()

            df3.write.partitionBy("display_name") \
                    .mode("overwrite") \
                    .parquet(output_path)

