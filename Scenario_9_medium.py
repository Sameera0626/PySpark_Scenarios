Find the 3 most profitable companies in the world. Sort the result based on profits in descending order. 
If multiple companies have the same profit, assign them the same rank and include all tied companies in the top results. 
Output the result along with the corresponding company name.

from pyspark.sql import SparkSession
from pyspark.sql.functions import*
from pyspark.sql.window import Window

spark=SparkSession.builder.appName("Topprofitablecompanies").getOrcreate()

#sample data
data = [
    ("Apple", 500000000),
    ("Microsoft", 500000000),
    ("Amazon", 450000000),
    ("Google", 400000000),
    ("Tesla", 350000000),
    ("Samsung", 450000000),
    ("Facebook", 400000000)
]

#define schema
columns=["company_name","profits"]

forbes_df=spark.createDataFrame(data,columns)
forbes_df.show()

#define a window specification to rank companies by profits in descending order
window_spec=Window.orderBy(col("profits")desc())

#add dense_rank column to rank companies
ranked_df=forbes_df.withColumn("rank",dense_rank().over(window_spec))

#filter top 3 ranks
top_companies_df=ranked_df.filter(col("rank") <= 3)

#specified the necessary columns
final_df=top_companies_df.select("company_name","profits","rank").orderBy("rank","profits" ,ascending=["True","False"])
final_df.show()

SQL

Solution:

with ranked_salary as(
  select company_name,profits,
  dense_rank() over (order by profits desc) as ranking
from forbes_global_2010_2014
)
select company_name,profits from ranked_salary
where ranking <= 3
order by profits desc;
