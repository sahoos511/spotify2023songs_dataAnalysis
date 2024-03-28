from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Extract, Transform, and Load CSV into Redshift") \
    .getOrCreate()

# Step 1: Get the CSV file from the inputs folder in the spotifytopsongs2023 bucket
bucket_name = "spotifytopsongs2023"
input_file_path = "inputs/spotify-2023.csv"
s3_input_path = f"s3://{bucket_name}/{input_file_path}"

# Read the CSV file from S3 into a DataFrame
df = spark.read.csv(s3_input_path, header=True, inferSchema=True)

# Step 2: Perform data transformation
percentage_columns = ["danceability_%", "valence_%", "energy_%", "acousticness_%", "instrumentalness_%", "liveness_%", "speechiness_%"]
for col in percentage_columns:
    new_col_name = col.replace("_%", "percentage")
    df = df.withColumnRenamed(col, new_col_name)
    df = df.withColumn(new_col_name, F.col(new_col_name).cast("float"))

df = df.withColumnRenamed("artist(s)_name", "artist_name")
df = df.withColumn("total_streams", F.col("streams").cast("long"))
df = df.drop("streams")
df = df.withColumn("bpm", F.col("bpm").cast("integer"))
df = df.withColumn("key", F.col("key").cast("integer"))

columns_to_filter = ["track_name", "artist_name", "released_year"]
df = df.filter(~F.col("track_name").isNull() & ~F.col("artist_name").isNull() & ~F.col("released_year").isNull())

# Step 3: Write the cleaned data into another CSV file in the outputs folder
folder_path = "outputs"
output_file_name = "cleaned_spotifySong_data.csv"
s3_output_path = f"s3://{bucket_name}/{folder_path}/{output_file_name}"
df.write.csv(s3_output_path, header=True, mode="overwrite")

# Step 4: Show the columns and data types in the cleaned CSV file
# Read the cleaned CSV file into a DataFrame
cleaned_df = spark.read.csv(s3_output_path, header=True, inferSchema=True)

# Display the schema (column names and data types)
cleaned_df.printSchema()

# Stop the Spark session
spark.stop()

redshift_url = "jdbc:redshift://redshift-cluster-1.co7pnt3adywm.us-east-1.redshift.amazonaws.com:5439/dev"
redshift_table = "spotify_songs"
redshift_properties = {
    "user": "awsuser",
    "password": "Apple1234",
    "driver": "com.amazon.redshift.jdbc42.Driver"
}

# Read the DataFrame from the cleaned CSV file
cleaned_df = spark.read.csv("s3://spotifytopsongs2023/outputs/cleaned_spotifySong_data.csv", header=True, inferSchema=True)

# Write DataFrame to Redshift
cleaned_df.write \
    .format("jdbc") \
    .option("url", redshift_url) \
    .option("dbtable", redshift_table) \
    .option("user", redshift_properties["user"]) \
    .option("password", redshift_properties["password"]) \
    .option("driver", redshift_properties["driver"]) \
    .mode("append") \
    .save()

# Stop the Spark session
spark.stop()
