from datetime import datetime, date, timedelta
from pyspark.sql.functions import *
from pyspark.sql.types import *

def get_date_hour_list():
    date_hour_list = []
    for i in range(0,23):
        data =  (datetime.now() - timedelta(hours=i)).strftime("%Y%m%d_%H%M")
        date_hour_list.append(str(data))
    return date_hour_list

def save_gold_data(date_hour_list):
    currency_list = ['btc', 'eth', 'ada']
    df_currency = spark.createDataFrame([], StructType([]))
    for currency in currency_list:
        for date in date_hour_list:
            try:
                df_currency_raw = spark.read.parquet(f"dbfs:/mnt/grupo1/streaming/silver/{currency}/{date[0:4]}/{date[4:6]}/{date[6:8]}/{date[9:11]}/")
                df_currency = df_currency.unionByName(df_currency_raw, allowMissingColumns=True)
            except Exception as e:
                print(e)

    df_currency = df_currency.withColumn('write_timestamp', lit(datetime.now()))
    df_currency.write.mode("overwrite").parquet(f"dbfs:/FileStore/criptomoeda/streaming/gold/")
    df_currency.write.mode("overwrite").parquet(f"/mnt/grupo1/streaming/gold/")


if __name__ == '__main__':
    save_gold_data()