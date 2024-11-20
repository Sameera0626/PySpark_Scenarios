Task:
Calculate the total number of unique active users per day.

SQL
-- Create login table
CREATE TABLE logins (
    user_id INT,
    login_date DATE
);

-- Insert sample data
INSERT INTO logins VALUES 
(1, '2024-11-01'),
(2, '2024-11-01'),
(3, '2024-11-01'),
(1, '2024-11-02'),
(2, '2024-11-02'),
(4, '2024-11-02'),
(5, '2024-11-03'),
(3, '2024-11-03');

select count(distinct user_id)as daily_active_user,login_date
from logins
group by login_date
order by login_date

#pyspark
data = [
    (1, "2024-11-01"),
    (2, "2024-11-01"),
    (3, "2024-11-01"),
    (1, "2024-11-02"),
    (2, "2024-11-02"),
    (4, "2024-11-02"),
    (5, "2024-11-03"),
    (3, "2024-11-03")
]

schema = StructType([
    StructField("user_id", IntegerType(), True),
    StructField("login_date", StringType(), True)
])

logins_df = spark.createDataFrame(data, schema=schema)
logins_df.show()

df=logins_df.groupBy("login_date).agg(
                     .countdistinct("user_"id").alias("daily_active_user)
                     .orderBy("login_date")
)
df.show()

