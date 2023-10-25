from pyspark.ml.feature import StringIndexer
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.feature import StandardScaler
from pyspark.ml.classification import LogisticRegression
from pyspark.ml import Pipeline
from pyspark.ml.tuning import ParamGridBuilder, CrossValidator
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Create a Spark session
spark = SparkSession.builder.appName("FraudDetection").getOrCreate()

# Load the dataset
df = spark.read.csv("Fraud.csv", header=True, inferSchema=True)

# Remove unnecessary columns
cols_to_remove = ["step", "nameOrig", "nameDest", "isFlaggedFraud"]
df = df.drop(*cols_to_remove)

# Remove 50% of isFraud == 0
fraudulent = df.filter(col("isFraud") == 1)
non_fraudulent = df.filter(col("isFraud") == 0).sample(fraction=0.5, seed=42)
df = fraudulent.union(non_fraudulent)

# Balance dataset by oversampling isFraud == 1
fraudulent_count = df.filter(col("isFraud") == 1).count()
non_fraudulent_count = df.filter(col("isFraud") == 0).count()

oversampling_ratio = non_fraudulent_count / fraudulent_count

df_oversampled = df.union(fraudulent.sample(True, oversampling_ratio, seed=42))

# Convert categorical variables to numerical representations
string_indexer = StringIndexer(inputCol="type", outputCol="type_index")
# df_oversampled = string_indexer.fit(df_oversampled).transform(df_oversampled)

# Vectorize features
feature_cols = ["amount", "oldbalanceOrg", "newbalanceOrig", "oldbalanceDest", "newbalanceDest", "type_index"]
vector_assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")
# df_oversampled = vector_assembler.transform(df_oversampled)

# Scale features
scaler = StandardScaler(inputCol="features", outputCol="scaled_features", withStd=True, withMean=True)
# scaler_model = scaler.fit(df_oversampled)
# df_oversampled_scaled = scaler_model.transform(df_oversampled)

# Split the data into training and testing sets
train_data, test_data = df_oversampled.randomSplit([0.8, 0.2], seed=42)

# Train the model (Logistic Regression)
lr = LogisticRegression(labelCol="isFraud", featuresCol="scaled_features")
pipeline = Pipeline(stages=[string_indexer, vector_assembler, scaler, lr])
model = pipeline.fit(train_data)

# Make predictions on the test set
predictions = model.transform(test_data)

# Evaluate the model
evaluator = BinaryClassificationEvaluator(labelCol="isFraud")
area_under_curve = evaluator.evaluate(predictions)
print(f"Area Under ROC Curve (AUC): {area_under_curve}")
# Save the trained model
model_path = "pre_trained_model"
model.save(model_path)

# Cleanup
spark.stop()