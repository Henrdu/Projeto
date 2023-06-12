from datetime import datetime, date, timedelta
from pyspark.sql.functions import *
from pyspark.sql.types import *

def save_gold_data():
    df_btc = spark.read.parquet(f"dbfs:/FileStore/criptomoeda/daily/silver/btc/")
    df_eth = spark.read.parquet(f"dbfs:/FileStore/criptomoeda/daily/silver/eth/")
    df_ada = spark.read.parquet(f"dbfs:/FileStore/criptomoeda/daily/silver/ada/")

    df_currency = df_btc.unionByName(df_eth, allowMissingColumns=True).unionByName(df_ada, allowMissingColumns=True)
    df_currency = df_currency.withColumn('write_timestamp', lit(datetime.now()))

    df_currency.write.mode("overwrite").parquet(f"dbfs:/FileStore/criptomoeda/daily/gold/")
    df_currency.write.mode("overwrite").parquet(f"/mnt/grupo1/daily/gold/")


if __name__ == '__main__':
    save_gold_data()