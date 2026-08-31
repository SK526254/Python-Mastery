from pyspark.sql import SparkSession
from pyspark.sql.types import *

from pyspark.sql.functions import *
from pyspark.sql.window import Window


spark  = SparkSession.builder.appName("pysparkfundamentals").getOrCreate()

orders_schema = StructType([
    StructField("order_id", IntegerType() ),
    StructField("customer_id", IntegerType()),
    StructField("date", DateType()),
    StructField("time", StringType()),
    StructField("order_timestamp", TimestampType()),
    StructField("product_id", IntegerType()),
    StructField("quantity", IntegerType()),
    StructField("unit_price", DecimalType(10, 2)),
    StructField("status", StringType()),
    StructField("country", StringType())
])

csv_df = (
            spark.read 
                .option("header", True) 
                .option("timestampFormat", "M/dd/yyyy HH:mm:ss")
                .option("dateFormat","M/dd/yyyy")
                .schema(orders_schema)
                .csv("./data/input/Orders_data.csv")
        )


validated_df = csv_df.select(
    col("order_id"), 
    col("customer_id"), 
    col("date"), 
    col("order_timestamp"), 
    col("product_id"), 
    col("status"), 
    col("country"), 
    col("unit_price"), 
    col("quantity"), 
    filter(array( when(col("order_id").isNull(), "Invalid: Null Order Id"),
        when(col("customer_id").isNull(), "Invalid: Null Cx Id"),
        when(col("order_timestamp").isNull(), "Invalid: Null TimeStamp "),
        when(col("product_id").isNull(), "Invalid: Null Product Id"),
        when(col("status").isNull(), "Invalid: Null status"),
        when(col("country").isNull(), "Invalid: Null COuntry"),
        when((col("unit_price").isNull()) | (col("unit_price") <= 0), "Invalid:  Unit price"),
        when((col("quantity").isNull()) | (col("quantity") <= 0), "Invalid: quantity")
        # when(row_number().over(latest_orders_window) > 1, "Invalid: Duplicate Record")
    ), lambda x : x.isNotNull()).alias("errors")
    # , row_number().over(latest_orders_window).alias("rn")
)



validated_df.cache()


# validated_df.show()

valid_data = validated_df.filter(array_size(col("errors")) == 0)
invalid_data = validated_df.filter(array_size(col("errors")) != 0)
# duplicate_data = invalid_data.filter(array_contains(col("errors"), "Invalid: Duplicate Record"))

latest_orders_window = Window.partitionBy("order_id").orderBy(desc("order_timestamp"))

valid_data = valid_data.withColumn("rn", row_number().over(latest_orders_window))

duplicate_data = valid_data.filter(col("rn")>1).withColumn("errors", array_append(col("errors"), "Duplicate Record")).drop("rn")
valid_data = valid_data.filter(col("rn")==1).drop("rn")

valid_data.show(truncate=False, n=1000)
invalid_data.show(truncate=False, n=1000)
duplicate_data.show(truncate=False, n=1000)

# print(valid_data.count())
# print(invalid_data.count())


valid_data.groupBy(col("date")).agg({"*":"count"}).select(count("date"), min("count(1)"), max("count(1)"), avg("count(1)")).show(n=1000)
valid_data.groupBy(col("customer_id")).agg({"*":"count"}).select(count("customer_id"), min("count(1)"), max("count(1)"), avg("count(1)")).show(n=1000)
valid_data.groupBy(col("order_timestamp")).agg({"*":"count"}).select(count("order_timestamp"), min("count(1)"), max("count(1)"), avg("count(1)")).show(n=1000)


# spark.stop()


# csv_df.select(
#     col("order_id"), 
#     sum(when(col("order_id").isNull(),1)
#         .otherwise(0)
        
#         ).alias("order_null_count"),
#     sum(when(col("customer_id").isNull(),1)
#         .otherwise(0)
        
#         ).alias("cx_null_count")
# ).show()

# csv_df.show(10)

# print(csv_df.count())

# print(csv_df.filter(csv_df.order_id.isNull()).count())

# df = spark.createDataFrame([{c:csv_df.filter(csv_df[c].isNull()).count()} for c in csv_df.columns]).show()