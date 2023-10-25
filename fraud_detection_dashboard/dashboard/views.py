import json
from django.shortcuts import render
from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv
from .models import KPI
import os

def kpi_view(request):
    load_dotenv()
    blob_service_client = BlobServiceClient.from_connection_string(os.environ['STORAGE_CONNECTION_STRING'])
    container_client = blob_service_client.get_container_client("frauddetection")
    blob_client = container_client.get_blob_client("results.json")

    blob_data = blob_client.download_blob()
    results_content = blob_data.readall().decode('utf-8')
    results = json.loads(results_content)

    kpi = KPI(results)
    return render(request, 'dashboard/kpi.html', {'kpi': kpi})