Task : 3rd highest salary

from pyspark.sql import SparkSession
from pyspark.sql.functions import*

spark=SparkSession.builder.appName("3rdhighestsalary").getOrCreate()

#sql
df.createOrReplaceTempView("highestsalary")

data = [("DEPT3", 500),
        ("DEPT3", 200),
        ("DEPT1", 1000),
        ("DEPT2", 100),
        ("DEPT2", 150),
        ("DEPT1", 700),
        ("DEPT1", 500),
        ("DEPT1", 600),
        ("DEPT2", 400),
        ("DEPT3", 250),
        ("DEPT3", 270),
        ("DEPT2", 200)]
columns = ["dept", "salary"]

df=spark.createDataFrame(data,columns)
df.show()

from pyspark.sql.window import Window
#window function
window_spec=Window.partitionBy("dept").orderBy("salary").desc())

df2=df.withColumn("drank",dense_rank().over(window_spec))

df2.filter(col("drank=3")).drop("drank").show()

#spark.sql
spark.sql("SELECT 
    department_id, 
    salary 
FROM (
    SELECT 
        department_id, 
        salary, 
        DENSE_RANK() OVER (PARTITION BY department_id ORDER BY salary DESC) AS rank 
    FROM employee
) ranked_salaries 
WHERE rank = 3;
