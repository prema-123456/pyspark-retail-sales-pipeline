from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, sum

# Step 1: Start Spark
spark = SparkSession.builder.appName("Retail Pipeline").getOrCreate()

# Step 2: Load Data
df = spark.read.csv("data/sales.csv", header=True, inferSchema=True)

# Step 3: Cleaning
df = df.dropna()
df = df.withColumn("date", to_date(col("date"), "yyyy-MM-dd"))

# Step 4: Feature Engineering
df = df.withColumn("total_price", col("price") * col("quantity"))

# =========================
# STEP 5: KPIs
# =========================

print("=== Total Revenue ===")
df.select(sum("total_price")).show()

# =========================
# STEP 6: Business Insights
# =========================

print("=== Category-wise Sales ===")
df.groupBy("category").sum("total_price").show()

print("=== City-wise Sales ===")
df.groupBy("city").sum("total_price") \
    .orderBy("sum(total_price)", ascending=False) \
    .show()

print("=== Top Products ===")
df.groupBy("product").sum("quantity") \
    .orderBy("sum(quantity)", ascending=False) \
    .show()

# Stop Spark
spark.stop()