communication_code	event_type
com1	              Sent
com2	              Open
com3	              Sent
com1	              Bounced
com3	              Bounced
com2	              Sent
com2	              Sent
com1	              Bounced
Output
======= 
communication_code Sent Open	Bounced
com1	             1	     0	      2
com2	             2	     1	      0
com3	             1	     0	      1

#PySpark

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, count

# Create Spark Session
spark = SparkSession.builder.master("local").appName("Communication Event").getOrCreate()

# Input Data
data = [
    ("com1", "Sent"),
    ("com2", "Open"),
    ("com3", "Sent"),
    ("com1", "Bounced"),
    ("com3", "Bounced"),
    ("com2", "Sent"),
    ("com2", "Sent"),
    ("com1", "Bounced")
]

columns = ["communication_code", "event_type"]
df=spark.createDataFrame(data,columns)
df.show()

#pivot to calculate the counts for each row

df1=df.groupBy("communicate_code").pivot("event_type").agg(count("*"))
df1.show()

#SQL

CREATE TABLE communication_events (
    communication_code VARCHAR(10),
    event_type VARCHAR(10)
);

-- Step 2: Insert Data
INSERT INTO communication_events VALUES 
('com1', 'Sent'),
('com2', 'Open'),
('com3', 'Sent'),
('com1', 'Bounced'),
('com3', 'Bounced'),
('com2', 'Sent'),
('com2', 'Sent'),
('com1', 'Bounced');

select communication_code,
sum(case when event_type = 'Sent' then 1 else 0 end ) as Sent,
sum(case when event_type = 'Open' then 1 else 0 end) as Open,
sum(case when event_type = 'Bounced' then 1 else 0 end) as Bounced
from communication_events
group by communication_code;
