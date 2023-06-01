from datetime import datetime, date, timedelta
from pyspark.sql.functions import *
from pyspark.sql.types import *

def get_date_hour_list():
    date_hour_list = []
    for i in range(0,23):
        data =  (datetime.now() - timedelta(hours=i)).strftime("%Y%m%d_%H%M")
        date_hour_list.append(str(data))
    return date_hour_list

def save_silver_data(date_hour_list, currency_name, currency_symbol):
    df_currency = spark.createDataFrame([], StructType([]))
    for date in date_hour_list:
        try:
            df_currency_raw = spark.read.option("multiline",True).option("mergeSchema",True).json(f"dbfs:/mnt/grupo1/streaming/raw/{currency_symbol}/{date[0:4]}/{date[4:6]}/{date[6:8]}/{date[9:11]}/")
            df_currency = df_currency.unionByName(df_currency_raw, allowMissingColumns=True)
        except Exception as e:
            print(e)

    df_currency = df_currency.select('data.price', 'data.timestamp')
    df_currency = df_currency.withColumn("reference_date", from_unixtime(col("timestamp").cast("string"),"yyyy-MM-dd HH:mm"))\
                             .withColumn("currency_name", lit(f'{currency_name}'))\
                             .withColumn("currency_symbol", lit(f'{currency_symbol}'))\
                             .withColumn("year", year("reference_date"))\
                             .withColumn("month", month("reference_date"))\
                             .withColumn("day", dayofmonth("reference_date"))\
                             .withColumn("hour", hour("reference_date"))\
                             .withColumn("minute", minute("reference_date"))\
                             .withColumn("dayofweek", dayofweek("reference_date"))\
                             .withColumn("counterparty_currency_name", lit("real"))\
                             .withColumn("counterparty_currency_symbol", lit("brl"))\
                             .withColumn("description", lit(f"conversao de {currency_name}({currency_symbol}) para real(brl)"))\
                             .withColumn("conversion_symbol", lit(f"{currency_symbol} x brl"))

    df_currency = df_currency.select(
                                    "conversion_symbol"
                                    ,"currency_name"
                                    ,"currency_symbol"
                                    ,"counterparty_currency_name"
                                    ,"counterparty_currency_symbol"
                                    ,"description"
                                    ,col("price").cast("decimal(10,3)").alias("conversion_value")
                                    ,"year"
                                    ,"month"
                                    ,"day"
                                    ,"dayofweek"
                                    ,"hour"
                                    ,"minute"
                                    ,lit(datetime.now()).alias("write_timestamp")  
                                    )
        
    # df_currency.write.partitionBy("year","month","day").mode("overwrite").parquet(f"dbfs:/FileStore/criptomoeda/streaming/silver/{currency_symbol}")
    # df_currency.write.partitionBy("year","month","day").mode("overwrite").parquet(f"/mnt/grupo1/streaming/silver/{currency_symbol}")


if __name__ == '__main__':
    date_hour_list = get_date_hour_list()
    save_silver_data(date_hour_list, currency_name='bitcoin', currency_symbol='btc')