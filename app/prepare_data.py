from pathvalidate import sanitize_filename
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, length
import os
import unicodedata
import re


INPUT_PATH = "file:///app/a.parquet"
OUTPUT_DIR = "/app/data"
num_docs = 1000

os.makedirs(OUTPUT_DIR, exist_ok=True)

spark = SparkSession.builder \
    .appName("PrepareData") \
    .master("local[*]") \
    .config("spark.sql.parquet.enableVectorizedReader", "true") \
    .getOrCreate()

def normalize_title(title):
    title = re.sub(r"\s+", " ", title).strip()
    title = sanitize_filename(title).replace(" ", "_")
    title = re.sub(r"_+", "_", title).strip("._")
    return title or "untitled"

df = spark.read.parquet(INPUT_PATH) \
    .select("id", "title", "text") \
    .filter(col("id").isNotNull()) \
    .filter(col("title").isNotNull()) \
    .filter(col("text").isNotNull()) \
    .filter(length(trim(col("text"))) > 0) \
    .limit(num_docs)

rows = df.collect()

for row in rows:
    title = normalize_title(row["title"])
    filename = os.path.join(OUTPUT_DIR, f"{row['id']}_{title}.txt")
    with open(filename, "w", encoding="utf-8") as f:
        f.write(row["text"])

spark.stop()

