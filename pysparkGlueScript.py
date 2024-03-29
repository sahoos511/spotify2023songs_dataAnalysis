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
df = df.select("track_name",  F.col("artist(s)_name").alias("artist_name"), F.col("streams").cast("bigint").alias("total_streams"))

# Show the DataFrame schema and first few rows
df.printSchema()
df.show()

# Define Redshift parameters
redshift_url = "jdbc:redshift://redshift-cluster-1.co7pnt3adywm.us-east-1.redshift.amazonaws.com:5439/dev"
redshift_table = "spotify_songs"
redshift_properties = {
    "user": "awsuser",
    "password": "Apple1234",
    "driver": "com.amazon.redshift.jdbc42.Driver"
}

# Write DataFrame to Redshift
df.write \
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
