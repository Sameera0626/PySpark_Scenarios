Employee ID  | EMployee Name
A101         | John F Thomas
B234         | Tom Moody Smith
T564         | Micheal K Sean
 
Output:
Employee ID  | Employee First Name |Employee Middle Name |Employee Last Name
A101         | John                |  F                  |  Thomas
B234         | Tom                 |  Moody              |  Smith
T564         | Micheal             |  K                  |  Sean


#Pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import*

spark=SparkSession.builder.appName("Employeedetails").getOrCreate()

data = [
    ("A101", "John F Thomas"),
    ("B234", "Tom Moody Smith"),
    ("T564", "Micheal K Sean")
]

# Create DataFrame
df = spark.createDataFrame(data, ["Employee ID", "Employee Name"])
df.show()

#seperating name iwth first name,middle,last

df1=df.withColumn("Employee First Name",split(df["Employee Name"]," ")[0])\
      .withColumn("Employee Middle Name",split(df["Employee Name"]," ")[1])\
      .withColumn("Employee Last Name", split(df["Employee Name"]," ")[2])\
      .drop ("Employee Name")
df1.show()



