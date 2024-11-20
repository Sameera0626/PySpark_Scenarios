t1 country
----------
India
Srilanka
Australia
Pakistan
 
t2 Country
--------
India
Srilanka
Australia
Pakistan
 
Output
=====
Country1   Country2
-------------------
India      Srilanka
India      Australia
India      Pakistan
Srilanka   Australia
Srilanka   Pakistan
Australia  Pakistan

#SQL
-- Step 1: Create the table
CREATE TABLE Countries (
    Country VARCHAR(50)
);

-- Step 2: Insert data into the table
INSERT INTO Countries (Country)
VALUES 
('India'),
('Srilanka'),
('Australia'),
('Pakistan');

select t1.country as Country1,
t2.country as Country2
from Countries t1
join
Countries t2
on t1.country < t2.country ;

#Pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Create Spark Session
spark = SparkSession.builder.master("local").appName("Country Pairing").getOrCreate()

# Input Data
data = [("India",), ("Srilanka",), ("Australia",), ("Pakistan",)]
columns = ["Country"]

# Create DataFrame
df = spark.createDataFrame(data, columns)
df.show()

#cross join and filter the unique pairs
df=df.alias("t1").crossjoin(df.alias("t2"))\
     .filter(col("t1.country") < col("t2.country")) \
     .select(col("t1.country").alias("country1"),col("t2.country").alias("country2"))
df.show()

             
