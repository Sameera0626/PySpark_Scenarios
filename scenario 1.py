 Class   name        gender
1          a          m
1          b          m
1          c          m
1          d          f
2          e          f
2          f           f
2          g          m

output should look like this👇🏻 :

class male female
1          3          1         
2          1          2         ??

#thorugh SQL
-- Create the table
CREATE TABLE class_data (
    class INT,
    name VARCHAR(50),
    gender CHAR(1)
);

-- Insert data into the table
INSERT INTO class_data (class, name, gender)
VALUES 
(1, 'a', 'm'),
(1, 'b', 'm'),
(1, 'c', 'm'),
(1, 'd', 'f'),
(2, 'e', 'f'),
(2, 'f', 'f'),
(2, 'g', 'm');

select class,
count(case when gender = 'm 'then 1 else 0 end) as male,
count(case when gender = 'f' then 1 else 0 end)as female
from class_data
group by class;

#through pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, count

# Initialize Spark Session
spark = SparkSession.builder.appName("Gender Count").getOrCreate()

# Sample Data
data = [
    (1, "a", "m"),
    (1, "b", "m"),
    (1, "c", "m"),
    (1, "d", "f"),
    (2, "e", "f"),
    (2, "f", "f"),
    (2, "g", "m")
]

columns = ["class", "name", "gender"]

df=spark.createDataFrame(data,columns)
df.show()

#groupby class and count male and female

df1=df.groupBy("class").agg(
      count(when(col("gender")=="m" , 1).alias("male"),
      count(when(col("gender")=="f",1).alias("female")
)   
df1.show()
      
      


