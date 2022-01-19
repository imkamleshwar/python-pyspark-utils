import datetime
import random

from functional import seq
from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit

spark = SparkSession.builder.appName("Playground").master("local[1]").getOrCreate()
spark.sparkContext.setLogLevel("WARN")
df = spark.range(10)
df.show(truncate=False)

col_tuple_list = [("Invoice Date", datetime.datetime.now()), ("Billing Account Number", random.randint(0, 100)),
                  ("Invoice Number", random.randint(10000, 20000))]

df_new: DataFrame = seq(col_tuple_list) \
    .fold_left(df,
               lambda current_df, col_and_value: current_df.withColumn(col_and_value[0], lit(col_and_value[1])))
df_new.show(truncate=False)

rename_col_tuple = [("Invoice Date", "invoice_date"), ("Billing Account Number", "billing_account_number"),
                    ("Invoice Number", "invoice_number")]

renamed_col_df = seq(rename_col_tuple) \
    .fold_left(df_new,
               lambda current_df, old_new_col_map: current_df.withColumnRenamed(old_new_col_map[0], old_new_col_map[1]))
renamed_col_df.show(truncate=False)

"""
+---+
|id |
+---+
|0  |
|1  |
|2  |
|3  |
|4  |
|5  |
|6  |
|7  |
|8  |
|9  |
+---+

+---+--------------------------+----------------------+--------------+
|id |Invoice Date              |Billing Account Number|Invoice Number|
+---+--------------------------+----------------------+--------------+
|0  |2022-01-19 17:34:28.962668|89                    |15471         |
|1  |2022-01-19 17:34:28.962668|89                    |15471         |
|2  |2022-01-19 17:34:28.962668|89                    |15471         |
|3  |2022-01-19 17:34:28.962668|89                    |15471         |
|4  |2022-01-19 17:34:28.962668|89                    |15471         |
|5  |2022-01-19 17:34:28.962668|89                    |15471         |
|6  |2022-01-19 17:34:28.962668|89                    |15471         |
|7  |2022-01-19 17:34:28.962668|89                    |15471         |
|8  |2022-01-19 17:34:28.962668|89                    |15471         |
|9  |2022-01-19 17:34:28.962668|89                    |15471         |
+---+--------------------------+----------------------+--------------+

+---+--------------------------+----------------------+--------------+
|id |invoice_date              |billing_account_number|invoice_number|
+---+--------------------------+----------------------+--------------+
|0  |2022-01-19 17:34:28.962668|89                    |15471         |
|1  |2022-01-19 17:34:28.962668|89                    |15471         |
|2  |2022-01-19 17:34:28.962668|89                    |15471         |
|3  |2022-01-19 17:34:28.962668|89                    |15471         |
|4  |2022-01-19 17:34:28.962668|89                    |15471         |
|5  |2022-01-19 17:34:28.962668|89                    |15471         |
|6  |2022-01-19 17:34:28.962668|89                    |15471         |
|7  |2022-01-19 17:34:28.962668|89                    |15471         |
|8  |2022-01-19 17:34:28.962668|89                    |15471         |
|9  |2022-01-19 17:34:28.962668|89                    |15471         |
+---+--------------------------+----------------------+--------------+

"""
