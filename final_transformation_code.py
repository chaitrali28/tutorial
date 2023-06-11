import sys
from awsglue.job import Job
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql.functions import *
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import SparkSession
from pyspark.sql.functions import when,regexp_replace,translate,trim,split,explode,col,substring
spark = SparkSession.builder.master("local[*]").appName("oracle connection").getOrCreate()

glueContext = GlueContext(spark)

# Get arguments passed to the job from the command line
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

# Create a new job with the provided name
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Read input data from a data source
df1 = spark.read.csv('s3://input-data-project/input_files/empUpdate.csv',inferSchema=True,header=True)
df2 = spark.read.csv('s3://input-data-project/input_files/personUpdate.csv',inferSchema=True,header=True)
df3= spark.read.csv('s3://input-data-project/input_files/empdeptUpdate.csv',inferSchema=True,header=True)


# Apply transformations to the data

dfemp=df1.withColumn("businessentityid",regexp_replace(col("businessentityid"),"[^\\d]",""))
dfperson=df2.withColumn("businessentityid",regexp_replace(col("businessentityid"),"[^\\d]",""))
dfempdept=df3.withColumn("businessentityid",regexp_replace(col("businessentityid"),"[^\\d]",""))

dfemp.createTempView("emp")
dfperson.createTempView("person")
dfempdept.createTempView("emp_dept")



dfjoin = spark.sql("""
    SELECT p.FirstName, p.LastName, p.MiddleName, p.NameStyle, p.Title,
        e.hirestring, e.birthstring, e.LoginID, e.MaritalStatus,
        e.Salariedflag, e.Gender, e.VacationHours, e.SeekLeaveHours,
        e.CurrentFlag, edh.DepartmentID, edh.StartDate, edh.EndDate
    FROM person p
    INNER JOIN emp e ON p.businessentityid = e.businessentityid
    INNER JOIN emp_dept edh ON p.businessentityid = edh.businessentityid
""")

dfjoin = dfjoin.repartition(1)
dfjoin = dfjoin.withColumn("FirstName", regexp_replace("FirstName", "\\+", ""))
dfjoin = dfjoin.withColumn("LastName", regexp_replace("LastName", "\\+", ""))
dfjoin = dfjoin.withColumn("MiddleName", regexp_replace("MiddleName", "\\+", ""))
dfjoin = dfjoin.withColumn("NameStyle", regexp_replace("NameStyle", "\\+", ""))
dfjoin = dfjoin.withColumn("Title", regexp_replace("Title", "\\+", ""))

print("Good Records")
gooddata = dfjoin.filter(~col("VacationHours").isNull()). \
    filter(~col("SeekLeaveHours").isNull())

print("Null Values....Bad Records")
baddata= dfjoin.subtract(gooddata)

# Write the transformed Null data to S3
dfjoin=dfjoin.coalesce(1)
dfjoin.write.mode('overwrite').option("header", "true").csv("s3://input-data-project/output/cleandata/dimemp.csv")
baddata=baddata.coalesce(1)
baddata.write.mode('overwrite').option("header", "true").csv("s3://input-data-project/output/BadRecords/badRecord.csv")
gooddata=gooddata.coalesce(1)
gooddata.write.mode('overwrite').option("header", "true").csv("s3://input-data-project/output/GoodRecords/Goodrecord.csv")



# Commit the job
job.commit()