# Real-Time Fraud Detection Project

## Overview

This project focuses on real-time fraud detection in financial transactions using Apache Spark, PySpark's MLlib, Kafka, and Azure services. The system processes streaming data, trains a Logistic Regression model, stores data and predictions in Azure Table Storage and provides insights through a simple Django web application.

## Table of Contents

- [Key Features](#key-features)
- [Tech Stack](#tech-stack)
- [Project Structure](#project-structure)
- [Setup](#setup)
- [Usage](#usage)
- [Contributing](#contributing)

## Key Features

1. **Logistic Regression Model:**
   - Trained a model using PySpark's MLlib for accurate fraud detection.

2. **Real-Time Data Flow:**
   - Simulated real-time data with Kafka.
   - Spark Streaming for real-time predictions.

3. **Azure Integration:**
   - Stored data and predictions in Azure Table Storage.
   - Azure Function for data extraction, analysis, and Blob Storage storage.

4. **Django Web App:**
   - Retrieves and visualizes fraud detection insights.

## Tech Stack

- Apache Spark
- PySpark's MLlib
- Apache Kafka
- Azure Table Storage
- Azure Functions
- Django Web App

## Project Structure
```
├── kafka_producer.py # Simulates real-time data with Kafka
├── spark_streaming_job.py # PySpark streaming job for real-time predictions
├── train_model.py # Trains Logistic Regression model
├── function_app.py # Azure HTTP-triggered function for analysis
├── host.json # for azure function
├── requirements.txt
├── fraud_detection_dashboard/ # Django web application
│ ├── dashboard/
│ ├── fraud_detection_dashboard/
│ └── manage.py
├── pre_trained_model/ # Pre-trained ML model
│ └── ...
├── README.md # Project documentation
```


## Setup

0. **Download Data**
   - Link: [Dataset](https://www.kaggle.com/datasets/chitwanmanchanda/fraudulent-transactions-data).

1. **Environment Setup:**
   - Install dependencies: `pip install -r requirements.txt`.

2. **Azure Configuration:**
   - Set up Azure Table Storage and Blob Storage.
   - Configure Azure connection details in relevant files.

3. **Model Training:**
   - Run `train_model.py` to train the Logistic Regression model.


## Usage

1. **Start Kafka Producer:**
   ```bash
   python kafka_producer.py

2. **Run Spark Streaming Job:**
   ```bash
   spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 spark_streaming_job.py

3. **Execute Azure Function:**
   - Deploy and trigger the Azure Function for additional analysis.

4. **Run Django Web App:**
   - Start the Django web application to visualize fraud detection insights.


## Contributing

Contributions are welcome! Feel free to open issues or submit pull requests.
