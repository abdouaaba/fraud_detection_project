import os
import azure.functions as func
from azure.data.tables import TableClient
from azure.storage.blob import BlobServiceClient
import pandas as pd
import json

types = ["CASH_IN", "CASH_OUT", "TRANSFER", "PAYMENT", "DEBIT"]

# Load data from Azure Table Storage
def load_data():
    table_service = TableClient.from_connection_string(os.environ['AzureStorageString'], "frauddetection")
    entities = dict()
    for transaction_type in types:
        entities[transaction_type] = pd.DataFrame(table_service.query_entities(query_filter=f"PartitionKey eq '{transaction_type}'",
                                                                  select=["Type", "Amount", "Prediction", "Probability"]))
    return entities

# Perform analysis
def perform_analysis(data):
    nb_transactions = 0
    total_amount = 0
    nb_fraud = 0
    nb_notfraud = 0
    for transaction_type in types:
        nb_transactions += data[transaction_type].count()[0]
        total_amount += sum(list(data[transaction_type]["Amount"]))
        nb_fraud += data[transaction_type][data[transaction_type]["Prediction"] == 1].count()[0]
        nb_notfraud += data[transaction_type][data[transaction_type]["Prediction"] == 0].count()[0]
    
    data = {
        "nb_transactions": int(nb_transactions),
        "nb_cash_in": int(data[types[0]].count()[0]),
        "nb_cash_out": int(data[types[1]].count()[0]),
        "nb_transfer": int(data[types[2]].count()[0]),
        "nb_payment": int(data[types[3]].count()[0]),
        "nb_debit": int(data[types[4]].count()[0]),
        "avg_amount": float(total_amount/nb_transactions),
        "nb_fraud": int(nb_fraud),
        "nb_notfraud": int(nb_notfraud)
    }
    return data


# Store results in Azure Storage
def store_results(results):
    blob_service_client = BlobServiceClient.from_connection_string(os.environ['AzureStorageString'])
    container_client = blob_service_client.get_container_client("frauddetection")
    blob_client = container_client.get_blob_client("results.json")

    # Convert results to JSON and store in Azure Storage
    blob_client.upload_blob(json.dumps(results), overwrite=True)

app = func.FunctionApp(http_auth_level=func.AuthLevel.ADMIN)

# HTTP-triggered function
@app.route(route="http_trigger")
def main(req: func.HttpRequest) -> func.HttpResponse:
    # Load data from Azure Table Storage
    data = load_data()

    # Perform analysis
    data = perform_analysis(data)

    # Store results in Azure Storage
    store_results(data)

    return func.HttpResponse("Analysis complete.")