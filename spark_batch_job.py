from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType
from pyspark.ml import PipelineModel

# Initialize Spark session
spark = SparkSession.builder \
    .appName("BatchFraudDetection") \
    .getOrCreate()

# Schema for the incoming JSON data
schema = StructType([
    StructField("transaction_id", IntegerType()),
    StructField("type", StringType()),
    StructField("amount", DoubleType()),
    StructField("oldbalanceOrg", DoubleType()),
    StructField("newbalanceOrig", DoubleType()),
    StructField("oldbalanceDest", DoubleType()),
    StructField("newbalanceDest", DoubleType()),
    StructField("isFraud", IntegerType())
])

# Read batch data for batch processing
batch_data = spark.read.format("csv").option("header", "true").load("/home/abdou/Desktop/fraud_detection/data/Fraud.csv")

# Load the pre-trained model
model = PipelineModel.load("pre_trained_model")

# Make predictions on the batch batch data
batch_predictions = model.transform(batch_data)

# Show the batch predictions
batch_predictions.select("transaction_id", "isFraud", "prediction", "probability").show(truncate=False)