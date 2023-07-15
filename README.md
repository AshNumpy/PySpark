# PySpark Cheat Sheet

This cheat sheet contains sample code, outputs, and tables for PySpark usage.

## Data Reading

```python
# Read CSV file
df = spark.read.csv('data.csv', header=True, inferSchema=True)

# Read Parquet file
df = spark.read.parquet('data.parquet')

# Read JSON file
df = spark.read.json('data.json')
```

## Data Exploration

```python
# Show the first 5 rows of the dataset
df.show(5)

# Print the schema information
df.printSchema()

# Describe statistics of a column
df.describe('column_name').show()
```

## Data Processing

```python
# Select a column
df.select('column_name')

# Add a column
df.withColumn('new_column', expr)

# Rename a column
df.withColumnRenamed('old_column', 'new_column')

# Filter rows based on a condition
df.filter(expr)

# Group by a column and calculate sum
df.groupBy('column_name').sum('numeric_column')
```

## Machine Learning

```python
# Split the dataset into features and label
feature_columns = ['feature1', 'feature2']
label_column = 'label'
data = df.select(feature_columns + [label_column])

# Split the data into training and test sets
train_data, test_data = data.randomSplit([0.7, 0.3], seed=42)

# Create and train a machine learning model
from pyspark.ml.regression import LinearRegression
lr = LinearRegression(featuresCol='features', labelCol='label')
lr_model = lr.fit(train_data)

# Make predictions on the test set
predictions = lr_model.transform(test_data)
predictions.show()
```

| Kcal | Miles |
|------|-------|
| 100  | 5     |
| 200  | 10    |
| 300  | 15    |