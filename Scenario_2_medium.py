
Find total sales and average sales amount per customer
#through SQL
CREATE TABLE sales (
    sale_id INT,
    customer_id INT,
    amount DECIMAL(10, 2),
    sale_date DATE
);

-- Insert sample data
INSERT INTO sales VALUES 
(1, 101, 200.50, '2024-11-01'),
(2, 102, 350.00, '2024-11-02'),
(3, 101, 150.00, '2024-11-02'),
(4, 103, 400.00, '2024-11-03'),
(5, 102, 250.00, '2024-11-03');


select customer_id,
sum(amount) as total_Sales ,
avg(amount) as avg_sales
from sales
group by customer_id;

#through pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField,IntegerType,DoubleType,DateType

#create Sparksession
spark=SparkSession.builder.appName("SalesAnalysis").getOrCreate()

## Define schema
schema = StructType([
    StructField("sale_id", IntegerType(), True),
    StructField("customer_id", IntegerType(), True),
    StructField("amount", DoubleType(), True),
    StructField("sale_date", DateType(), True)
])

# Create DataFrame
data = [
    (1, 101, 200.50, "2024-11-01"),
    (2, 102, 350.00, "2024-11-02"),
    (3, 101, 150.00, "2024-11-02"),
    (4, 103, 400.00, "2024-11-03"),
    (5, 102, 250.00, "2024-11-03")
]

df = spark.createDataFrame(data, schema=schema)
df.show()

from pyspark.sql.functions import*

result_df = df.groupBy("customer_id") \
              .agg(
                  sum("amount").alias("total_sales"),
                  avg("amount").alias("avg_sales")
              )
result_df.show()
