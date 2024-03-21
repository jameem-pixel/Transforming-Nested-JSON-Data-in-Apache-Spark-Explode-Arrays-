from pyspark.sql import SparkSession
from pyspark.sql.functions import udf,explode,col
from pyspark.sql.types import StringType,ArrayType, MapType

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("Example") \
    .getOrCreate()

data = [{'emp_id': 12, 'Name': 'Jameem'},{'emp_id':13,'Name':"Abdul"}]
df = spark.createDataFrame(data)
df.show()
def process_row(name):
    list_data = []
    if name == 'Jameem':
        list_data.append({"emp_id":12,"project1":"Data Engineerr", "project2":"ML"})
        return list_data
    elif name == 'Abdul':
        list_data.append({"emp_id":13,"project1":"Solution architech", "project2":"Data Engineer"})
        return list_data
    else:
        return None
schema = ArrayType(MapType(StringType(), StringType()))
row_udf = udf(process_row, schema)
df = df.withColumn("Projects", row_udf('Name'))
exploded_df = df.select("Name", explode("Projects").alias("dict"))
final_df = exploded_df.select(
'Name',
exploded_df["dict"]["emp_id"].alias("emp_id"),
exploded_df["dict"]["project1"].alias("project1"),
exploded_df["dict"]["project2"].alias("project2")
                            )
final_df.show(truncate=False)


