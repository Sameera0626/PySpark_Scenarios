Task:
Count the number of employees in each department and find the department with the highest average salary.

#through SQL
-- Create employee table
CREATE TABLE employees (
    emp_id INT,
    emp_name VARCHAR(50),
    dept_id INT,
    salary DECIMAL(10, 2)
);

-- Insert sample data
INSERT INTO employees VALUES 
(1, 'Alice', 1, 5000),
(2, 'Bob', 2, 7000),
(3, 'Charlie', 1, 5500),
(4, 'David', 3, 8000),
(5, 'Eve', 2, 6500);

select dept_id,
count(*) as total_employees,
avg(salary) as avg_salary
from employees
group by dept_id
order by avg_salary desc
limit 1;

#through pyspark
# Create DataFrame
data = [
    (1, 'Alice', 1, 5000),
    (2, 'Bob', 2, 7000),
    (3, 'Charlie', 1, 5500),
    (4, 'David', 3, 8000),
    (5, 'Eve', 2, 6500)
]

schema = StructType([
    StructField("emp_id", IntegerType(), True),
    StructField("emp_name", StringType(), True),
    StructField("dept_id", IntegerType(), True),
    StructField("salary", DoubleType(), True)
])

emp_df = spark.createDataFrame(data, schema=schema)
emp_df.show()

#pyspark query
result_df=emp_df.groupBy("dept_id").agg(
     count(*).alias("num_employees"),
     avg("salary").alias("avg_salary")
)
result_df.show()

#order by highest salary

result_df1=result_df.orderBy(desc("avg_salary").limit(1)
result_df1.show()


