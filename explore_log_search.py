import datetime

from unidecode import unidecode
import time
from pyspark.sql import SparkSession, Window, functions as F, types as T

import json

import unicodedata, regex as re

from gemini_api import call_gemini_api_with_retry

import os
from dotenv import load_dotenv

load_dotenv()

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

BIGQUERY_DATASET = "customer_360"
BIGQUERY_CLEANED_DATA_TABLE = "cleaned_log_search"
PROJECT_ID = "inner-suprstate-469409-a8"

BIGQUERY_OLAP_TABLE = 'trending'

# Danh sách thể loại của bạn

CATEGORY_LIST = [
    'Anime', 'Bí Ẩn', 'Chiến Tranh', 'Chiếu Rạp', 'Chuyển Thể', 'Chính Kịch',
    'Chính Luận', 'Chính Trị', 'Chương Trình Truyền Hình', 'Cung Đấu',
    'Cuối Tuần', 'Cách Mạng', 'Cổ Trang', 'Cổ Tích', 'Cổ Điển', 'DC', 'Disney',
    'Gay Cấn', 'Gia Đình', 'Giáng Sinh', 'Giả Tưởng', 'Hoàng Cung', 'Hoạt Hình',
    'Hài', 'Hành Động', 'Hình Sự', 'Học Đường', 'Khoa Học', 'Kinh Dị',
    'Kinh Điển', 'Kịch Nói', 'Kỳ Ảo', 'LGBT+', 'Live Action', 'Lãng Mạn',
    'Lịch Sử', 'Marvel', 'Miền Viễn Tây', 'Nghề Nghiệp', 'Người Mẫu',
    'Nhạc Kịch', 'Phiêu Lưu', 'Phép Thuật', 'Siêu Anh Hùng', 'Thiếu Nhi',
    'Thần Thoại', 'Thể Thao', 'Truyền Hình Thực Tế', 'Tuổi Trẻ', 'Tài Liệu',
    'Tâm Lý', 'Tập Luyện', 'Viễn Tưởng', 'Võ Thuật', 'Xuyên Không', 'Đau Thương',
    'Đời Thường', 'Ẩm Thực'
]

spark = (SparkSession.builder.appName("Explore Log Search")
        .config("spark.jars.packages", "graphframes:graphframes:0.8.1-spark3.0-s_2.12,"
                                       "com.google.cloud.spark:spark-3.5-bigquery:0.42.2")
         .getOrCreate())
spark.sparkContext.setCheckpointDir("./checkpoint/graphframes_checkpoints")

def remove_vietnamese_accents(text):
    if text is None:
        return None
    return unidecode(text)
# Đăng ký hàm dưới dạng UDF
remove_accents_udf = F.udf(remove_vietnamese_accents, T.StringType())

def read_parquet(path):
    df = spark.read.parquet(path)
    return df

def read_csv(path):
    df = spark.read.csv(path)
    return df

def select_col(df):
    df = df.select("user_id", "keyword" ).orderBy("user_id")

    return df

def remove_null(df):
    df = df.dropna(subset =['keyword'])
    df = df.fillna("UNKNOWN")
    return df

def add_date_col(df, date):
    df_with_datetime = df.withColumn(
        "date",

        F.lit(date).cast(T.DateType())
    )

    return df_with_datetime


def is_obviously_nonsense(q: str) -> bool:
    # Đảm bảo q là string và xử lý None hoặc không phải string
    if not isinstance(q, str):
        return True # Coi non-string là vô nghĩa

    q = q.strip()
    if not q:
        return True

    # 1. quá ngắn
    if len(q) == 1:
        return True

    # 2. toàn bộ là ký tự giống nhau (4 ký tự trở lên)
    if re.fullmatch(r'(.)\1{3,}', q):
        return True

    # 3. quá nhiều ký hiệu (ít hơn 40% là chữ cái/số)
    letters_digits = sum(ch.isalnum() for ch in q)
    if letters_digits / max(1, len(q)) < 0.4:
        return True

    return False

is_nonsense_udf = F.udf(is_obviously_nonsense, T.BooleanType())

def normalize_text(s: str) -> str:
    if not s: return ""
    s = unicodedata.normalize("NFKC", s).strip()
    s = re.sub(r"^[#@&\s]+", "", s)                         # bỏ tiền tố # @ &
    s = re.sub(r"[‘’‚‛“”„‟\"'`]+", "", s)                   # bỏ ngoặc kép/nháy
    s = re.sub(r"[^\p{L}\p{N}\s]+", " ", s)                 # bỏ ký tự phi chữ/số
    s = re.sub(r"\s+", " ", s).strip()
    return s

normalize_text_udf = F.udf(normalize_text, T.StringType())

def remove_trash_keyword(df):
    df_with_check = df.withColumn(
        "is_nonsense",
        is_nonsense_udf(F.col("keyword"))
    )
    df_cleaned = df_with_check.filter(F.col("is_nonsense") == F.lit(False))
    df_cleaned = df_cleaned.drop("is_nonsense")
    return df_cleaned

def normalize_keyword(df):
    df = df.withColumn(
        "normalized_keyword",
        normalize_text_udf(F.col("keyword"))
    )
    df = df.drop("keyword")
    df = df.withColumnRenamed("normalized_keyword", "keyword")

    return df


def get_batch_categories(keywords, category_list=CATEGORY_LIST):

    """
    Phân loại một danh sách các từ khóa trong một lần gọi API duy nhất.

    Args:
        keywords (list): Danh sách các từ khóa cần phân loại.
        category_list (list): Danh sách các thể loại có sẵn.

    Returns:
        dict: Một từ điển (dictionary) chứa kết quả {từ_khóa: thể_loại}.
    """
    keyword_str = "\n".join(f"- {kw}" for kw in keywords)
    category_str = ", ".join(category_list)

    prompt = f"""
    Bạn là một chuyên gia phân loại phim và chương trình truyền hình.
    Hãy phân loại mỗi từ khóa trong danh sách sau:
    {keyword_str}

    Chỉ chọn một thể loại duy nhất từ danh sách sau:
    {category_str}

    Nếu không có thể loại nào phù hợp, hãy trả lời chính xác là 'Unknown'.
    Vui lòng trả về kết quả dưới dạng chuỗi csv với cấu trúc:
    {{
        "từ khóa 1": "thể loại",
        "từ khóa 2": "thể loại",
        ...
    }}    
    """

    response = call_gemini_api_with_retry(prompt)
    if response is None:
        return None
    else:
        try:
            def extract_top_object(s: str) -> str:
                start = s.find('{')
                if start < 0: raise ValueError("Không thấy '{'")
                depth = 0
                for i, ch in enumerate(s[start:], start):
                    if ch == '{':
                        depth += 1
                    elif ch == '}':
                        depth -= 1
                        if depth == 0:
                            return s[start:i + 1]
                raise ValueError("Ngoặc không cân bằng")

            json_movie_with_category = extract_top_object(response.text.strip())
            print(json_movie_with_category)
            return json.loads(json_movie_with_category)
        except Exception as e:
            print(f"Can't parse JSON from api response: {e}")

            return None

def find_movie_category(df):
    # run quá quota gemini api rồi, hình như là nhanh quá, sleep 15 30s gì đó cho chắc

    df_movie_name = df.groupBy("keyword").count()

    df_movie_name = df_movie_name.select("keyword", F.row_number().over(Window.orderBy("keyword")).alias("rn"))
    n = 500
    total_rows = df_movie_name.count()

    movie_category_df = None

    while n < total_rows:

        movie_category_lst = []

        top_500 = df_movie_name.filter((F.col('rn') > n - 500) & ( F.col('rn') <= n)).select("keyword")
        # Collect the DataFrame as a list of Row objects
        rows = top_500.collect()

        # Convert each Row object to a string
        top_500_lst = [f"{row.keyword}" for row in rows]
        print(top_500_lst)
        movie_category_json = get_batch_categories(top_500_lst)
        for key, value in movie_category_json.items():
            movie_category_lst.append({"movie": key, "category": value})

        if movie_category_df:
            movie_category_df = movie_category_df.union(spark.createDataFrame(movie_category_lst))
        else:
            movie_category_df = spark.createDataFrame(movie_category_lst)

        n += 500
        time.sleep(10)


    return movie_category_df


def count_duplicate_keyword_by_user(df):
    df = df.groupBy("keyword", "user_id").agg(F.count("*").alias("search_times"))

    return df

def clean_log_search(date_from, date_to):
    result = None
    for i in range((date_to - date_from).days + 1):
        date = (date_from + datetime.timedelta(days=i))

        date_str = datetime.datetime.strftime(date, '%Y%m%d')
        # print(date_str)
        parquet_path = os.path.join(BASE_DIR, f'/mnt/hgfs/shared_vmware/data/log_search/log_search/{date_str}')

        if not os.path.exists(parquet_path):
            print('File does not exist: ' , parquet_path)
            continue

        print("------ Read from parquet ------")
        df = read_parquet(parquet_path)
        # df.show()

        print(df.count())
        print("--------select column -------")

        df = select_col(df)
        df = remove_null(df)
        df = remove_trash_keyword(df)
        df = normalize_keyword(df)
        df = count_duplicate_keyword_by_user(df)
        df = add_date_col(df, date)
        df.printSchema()
        # df.write.mode("overwrite").parquet('./cleaned_data/dates/'+date_str)

        # write_to_bigquery(df)

        if result is None:
            result = df
        else:
            result = result.union(df)
    result.show(truncate=False)
    return result


def top_trending_per_user(df_user_log_search):
    df_user_log_search = df_user_log_search.withColumn("top", F.row_number().over(
        Window.partitionBy("user_id").orderBy("count")))
    df_user_log_search.show()
    top3_most_search_each_user = df_user_log_search.orderBy("user_id", "top").filter("top <= 3")
    top3_most_search_each_user.show(truncate=False)
    print(top3_most_search_each_user.count())
    return top3_most_search_each_user

def top3_most_search_each_user_with_category(top3_most_search_each_user, category_movie: list):

    category_movie_df = spark.createDataFrame(category_movie)
    category_movie_df.printSchema()

    result = top3_most_search_each_user.join(category_movie_df,
                                             top3_most_search_each_user.keyword == category_movie_df.movie,
                                             how="left")

    result = result.fillna({"category": "Unknown"})
    result = result.drop("movie")

    return result

#
# def write_to_bigquery(df):
#     full_table_path = f"{PROJECT_ID}.{BIGQUERY_DATASET}.{BIGQUERY_CLEANED_DATA_TABLE}"
#
#     df_bigquery = df.select(F.col("user_id").cast(T.StringType()),
#                              F.col("keyword").alias("keyword"),
#                              F.col("search_times").alias("search_times"),
#                              F.col("date").alias("datetime"))
#
#
#     # df_bigquery.printSchema()
#     (df_bigquery.write.format("bigquery")
#      .option("table", full_table_path)
#      .option("writeMethod", "direct")
#      .option("parentProject", PROJECT_ID)
#      .option("ignoreExtraFields", "true")
#      .mode("append").save())
#
#     return "Ghi thành công"



def write_table_log_search_with_cluster_id():
    datestr = '20220601'
    table_name = 'cleaned_log_search_with_cluster_id'
    df = spark.read.parquet(f'./cleaned_data/keyword_with_category/{datestr}')

    df_bigquery = df.select(F.col("keyword").alias("keyword"),
                            F.col("cluster_id").alias("cluster_id"))

    full_table_path = f"{PROJECT_ID}.{BIGQUERY_DATASET}.{table_name}"

    (df_bigquery.write.format("bigquery")
     .option("table", full_table_path)
     .option("writeMethod", "direct")
     .option("parentProject", PROJECT_ID)

     .mode("append").save())
    return True


def write_table_keyword_category():
    monthstr = '202206'
    table_name = 'keyword_category'
    df = spark.read.csv(f'./cleaned_data/keyword_with_category/offset_api_call_202206' , header=True)

    df_bigquery = df.select(F.col("movie").alias("keyword"),
                            F.col("category").alias("category"))

    full_table_path = f"{PROJECT_ID}.{BIGQUERY_DATASET}.{table_name}"

    (df_bigquery.write.format("bigquery")
     .option("table", full_table_path)
     .option("writeMethod", "direct")
     .option("parentProject", PROJECT_ID)

     .mode("append").save())
    return True

def pivot_olap_table(log_search_t6_df, log_search_t7_df , keyword_category_df):
    ## co log search , keyword & category call tu api gemini,
    # -> mapping ra  dang bang : user_id, most_search_t6, category_t6,	most_search_t7,	category_t7,	Trending_Type,	Previous

    most_search_t6 = log_search_t6_df.withColumn("top", F.row_number().over(Window.partitionBy("user_id").orderBy("search_times"))).filter("top = 1")

    most_search_t7 = log_search_t7_df.withColumn("top", F.row_number().over(
        Window.partitionBy("user_id").orderBy("search_times"))).filter("top = 1")

    most_search_t6_with_category = most_search_t6.join(keyword_category_df, on = most_search_t6['keyword'] == keyword_category_df['movie'], how="left").select(most_search_t6.user_id, 'keyword', 'category')

    most_search_t7_with_category = most_search_t7.join(keyword_category_df,
                                                       on=most_search_t7['keyword'] == keyword_category_df['movie'],
                                                       how="left").select(most_search_t7.user_id, 'keyword', 'category')
    trending_df= most_search_t6_with_category.alias('a').join(most_search_t7_with_category.alias('b'),
                                        on="user_id",
                                        how="left").select('a.user_id',
                                                           F.col('a.keyword').alias('most_search_t6'),
                                                           F.col('a.category').alias('category_t6'),
                                                           F.col('b.keyword').alias('most_search_t7'),
                                                           F.col('b.category').alias('category_t7')).withColumn("trending_type", F.when(F.col('category_t6') == F.col('category_t7'), "unchanged").otherwise("changed") )

    trending_df.printSchema()
    trending_df.fillna("Unknown")
    # trending_df.filter( 'category_t6 != "Unknown" and category_t7 != "Unknown"' ).show(truncate=False)
    # most_search_t6.printSchema()
    # most_search_t6_with_category.printSchema()


    return trending_df

def write_to_bigquery(df , table_name):
    full_table_path = f"{PROJECT_ID}.{BIGQUERY_DATASET}.{table_name}"

    # df_bigquery.printSchema()
    (df.write.format("bigquery")
     .option("table", full_table_path)
     .option("writeMethod", "direct")
     .option("parentProject", PROJECT_ID)
     .option("ignoreExtraFields", "true")
     .mode("append").save())

#


def main():

    date_from_t6 = datetime.datetime(2022, 6, 1)
    date_to_t6 = datetime.datetime(2022, 6, 1)


    date_from_t7 = datetime.datetime(2022, 7, 1)
    date_to_t7 = datetime.datetime(2022, 7, 1)

    df_cleaned_t6 = clean_log_search(date_from_t6, date_to_t6)


    keyword_by_user_times_t6 = count_duplicate_keyword_by_user(df_cleaned_t6)


    df_cleaned_t7 = clean_log_search(date_from_t7, date_to_t7)
    df_cleaned_data =df_cleaned_t6.union(df_cleaned_t7)

    df_cleaned_data.write.mode("overwrite").option("header", True).partitionBy("date").parquet('./destination_data/cleaned_data/')

    keyword_by_user_times_t7 = count_duplicate_keyword_by_user(df_cleaned_t7)

    print('----- select distinct keyword to find category ------')
    keyword_distinct = keyword_by_user_times_t6.union(keyword_by_user_times_t7).select("keyword").distinct()


    ## limit keyword api call for testing
    keyword_distinct = keyword_distinct.limit(1000)
    ## end
    keyword_category_df = find_movie_category(keyword_distinct)

    keyword_category_df.write.mode("overwrite").option("header", True).partitionBy("category").parquet('./destination_data/keyword_with_category')

    df_olap = pivot_olap_table( keyword_by_user_times_t6, keyword_by_user_times_t7, keyword_category_df)

    write_to_bigquery(df_olap , BIGQUERY_OLAP_TABLE)



if __name__ == "__main__":
    main()

