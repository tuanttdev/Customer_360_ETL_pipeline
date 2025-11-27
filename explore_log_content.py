
from pyspark.sql.functions import *
from pyspark.sql import SparkSession, Window, functions as F, types as T

import datetime
import os
from dotenv import load_dotenv

load_dotenv()
service_account_key_path = os.getenv('service_account_key_path')


spark = (SparkSession.builder.config("spark.driver.memory", "8g")
            .config("spark.jars.packages", "com.google.cloud.spark:spark-3.5-bigquery:0.42.2")
            .config("spark.hadoop.google.cloud.auth.service.account.enable", "true")
            .config("spark.hadoop.google.cloud.auth.service.account.json.keyfile", service_account_key_path)
            .getOrCreate())


SAVE_PATH = './clean_data'
SOURCE_PATH = './data/log_content'

PROJECT_ID = os.getenv('PROJECT_ID',"radiant-saga-478604-m9")
BIGQUERY_DATASET = os.getenv('BIGQUERY_DATASET',"customer_360")

def create_file_path(folder_path, datefrom:str, dateto:str):

    files_path = []

    for file in os.listdir(folder_path):
        if file.split('.')[0].isdigit() and  int(file.split('.')[0]) >= int(datefrom) and int(file.split('.')[0]) <= int(dateto):
            files_path.append(os.path.join(folder_path, file))

    return files_path

def read_data_from_path(path):
    df = spark.read.json(path)
    return df


def select_fields(df):
    df = df.select("_source.*")
    return df


def calculate_devices(df):
    total_devices = df.select("Contract", "Mac").groupBy("Contract").count()
    total_devices = total_devices.withColumnRenamed('count', 'TotalDevices')
    return total_devices


def transform_category(df):
    df = df.withColumn("Type",
                       when((col("AppName") == 'CHANNEL') | (col("AppName") == 'DSHD') | (col("AppName") == 'KPLUS') | (
                                   col("AppName") == 'KPlus'), "Truyền Hình")
                       .when(
                           (col("AppName") == 'VOD') | (col("AppName") == 'FIMS_RES') | (col("AppName") == 'BHD_RES') |
                           (col("AppName") == 'VOD_RES') | (col("AppName") == 'FIMS') | (col("AppName") == 'BHD') | (
                                       col("AppName") == 'DANET'), "Phim Truyện")
                       .when((col("AppName") == 'RELAX'), "Giải Trí")
                       .when((col("AppName") == 'CHILD'), "Thiếu Nhi")
                       .when((col("AppName") == 'SPORT'), "Thể Thao")
                       .otherwise("Error"))
    return df


def calculate_statistics(df):
    statistics = df.select('Contract', 'TotalDuration', 'Type').groupBy('Contract', 'Type').sum()
    statistics = statistics.withColumnRenamed('sum(TotalDuration)', 'TotalDuration')
    # find most watch type

    most_watch = (statistics.select("Contract" , "Type", F.row_number().over(Window.partitionBy("Contract").orderBy(F.col("TotalDuration").desc())).alias("most_watch")))

    statistics = (statistics.alias('a').join(most_watch.filter("most_watch = 1").alias('b'), on="Contract" , how="inner")
                  .select(F.col("a.Contract"), F.col("a.Type"), F.col("a.TotalDuration"), F.col("b.Type").alias("most_watch") ))

    # get top 3 type each contract
    top3_most_watch = most_watch.filter(F.col("most_watch") <= 3)
    df_taste = top3_most_watch.groupBy("Contract").agg(
        F.concat_ws(" - ", F.collect_list(F.col("Type"))).alias("taste")
    )
    statistics = statistics.alias('a').join(df_taste.alias('b'), on="Contract" , how="inner")

    statistics = statistics.withColumn('TotalDuration' , F.round(F.col('TotalDuration')/3600, 1))
    statistics = statistics.groupBy('Contract', 'most_watch', 'taste').pivot('Type').sum('TotalDuration').na.fill(0)
    return statistics


def finalize_result(statistics, total_devices):
    result = statistics.join(total_devices, 'Contract', 'inner')
    return result


def save_data(result, save_path):
    result.repartition(1).write.mode('overwrite').option("header", "true").csv(save_path)
    return print("Data Saved Successfully")


def write_table_viewing_time_analysis(df, table_name = 'viewing_time'):

    df_bigquery = df.select(F.col("contract").alias("contract"),
                            F.col("Giải Trí").alias("giai_tri"), F.col("Phim Truyện").alias("phim_truyen"),
                            F.col("Thiếu Nhi").alias("thieu_nhi"), F.col("Thể Thao").alias("the_thao"),
                            F.col("Truyền Hình").alias("truyen_hinh"), F.col("most_watch").alias("most_watch"), F.col("taste").alias("taste"))

    full_table_path = f"{PROJECT_ID}.{BIGQUERY_DATASET}.{table_name}"

    (df_bigquery.write.format("bigquery")
     .option("table", full_table_path)
     .option("writeMethod", "direct")
     .option("parentProject", PROJECT_ID)
     .mode("append").save())
    return True

def main(path, save_path):
    start_time = datetime.datetime.now()

    datefrom = '20220401'
    dateto = '20220401'
    file_paths = create_file_path(path, datefrom, dateto)

    print(file_paths)
    return
    print('-------------Reading data from path--------------')
    df = read_data_from_path(file_paths)
    df.show()
    print('-------------Selecting fields--------------')
    df = select_fields(df)
    df.show()
    print('-------------Calculating Devices --------------')
    total_devices = calculate_devices(df)
    total_devices.show()
    print('-------------Transforming Category --------------')
    df = transform_category(df)
    df.show()
    print('-------------Calculating Statistics --------------')
    statistics = calculate_statistics(df)
    statistics.show()

    print('-------------Finalizing result --------------')
    result = finalize_result(statistics, total_devices)
    result.show()
    print('-------------Saving Results --------------')
    write_table_viewing_time_analysis(result) ## to bigquery table
    # save_data(result, save_path) ## to local file

    endtime = datetime.datetime.now()
    timedelta = endtime - start_time
    print(timedelta.total_seconds())

    return print('Task finished')

if __name__ == '__main__':

    main(SOURCE_PATH, SAVE_PATH )