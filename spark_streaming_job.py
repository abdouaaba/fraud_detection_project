from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType
from pyspark.ml import PipelineModel
from pyspark.sql.types import StringType

from azure.data.tables import TableServiceClient, UpdateMode
import os
from dotenv import load_dotenv

# Initialize Spark session
spark = SparkSession.builder \
    .appName("StreamingFraudDetection") \
    .getOrCreate()

# Load the pre-trained model
model = PipelineModel.load("pre_trained_model")

# Schema for the incoming JSON data
schema = StructType([
    StructField("transaction_id", IntegerType()),
    StructField("type", StringType()),
    StructField("amount", DoubleType()),
    StructField("oldbalanceOrg", DoubleType()),
    StructField("newbalanceOrig", DoubleType()),
    StructField("oldbalanceDest", DoubleType()),
    StructField("newbalanceDest", DoubleType()),
])

# Read data from Kafka
streaming_data = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "financial_transactions") \
    .load()

# Convert the value column from Kafka into a string
json_data = streaming_data.selectExpr("CAST(value AS STRING)")

# Parse the JSON data
parsed_data = json_data.select(from_json(json_data.value, schema).alias("data")).select("data.*")

# Load the pre-trained model
model = PipelineModel.load("pre_trained_model")

# Predict
real_time_predictions = model.transform(parsed_data)

load_dotenv()
storage_connection_string = os.environ.get("STORAGE_CONNECTION_STRING")

def write_to_azure_storage(predictions_df, epoch_id):
    table_service_client = TableServiceClient.from_connection_string(storage_connection_string)
    table_client = table_service_client.get_table_client(table_name="frauddetection")

    # Convert DataFrame to a list of dictionaries
    data_list = predictions_df.toPandas().to_dict(orient='records')

    # Send data to the table
    for row in data_list:
        # Add logic to create a unique RowKey
        row_key = str(row['transaction_id'])

        if int(row['prediction']) == 0:
            prob = float(list(row['probability'].toArray())[0])
        else:
            prob = float(list(row['probability'].toArray())[1])
        # Create the entity
        entity = {
            'PartitionKey': str(row['type']), 
            'RowKey': row_key,
            'Type': str(row['type']),
            'Amount': float(row['amount']),
            'OldBalanceOrg': float(row['oldbalanceOrg']),
            'NewBalanceOrig': float(row['newbalanceOrig']),
            'OldBalanceDest': float(row['oldbalanceDest']),
            'NewBalanceDest': float(row['newbalanceDest']),
            'Prediction': int(row['prediction']),
            'Probability': prob,
        }

        # Insert or replace the entity in the table
        table_client.upsert_entity(entity=entity, mode=UpdateMode.MERGE)


# Output the real-time predictions to the console
query = real_time_predictions \
    .writeStream \
    .outputMode("append") \
    .foreachBatch(write_to_azure_storage) \
    .start()

# Await termination of the query
query.awaitTermination()