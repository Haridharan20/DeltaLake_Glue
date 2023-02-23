import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from awsglue.job import Job
from delta.tables import *

args = getResolvedOptions(sys.argv, ["JOB_NAME"])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)

job.init(args["JOB_NAME"], args)
spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")

table_path = 's3://sxm-delta-poc/deltaLake/delta_table'

deltaTable = DeltaTable.forPath(spark, table_path)

def create_table():
    df = spark.read.format('csv').load("s3://sxm-delta-poc/input/addresses.csv")
    df.write.format("delta").mode('overwrite').save('s3://sxm-delta-poc/deltaLake/delta_table/')
    
def insert_data():
    
    value = ("sai", "purushoth","345","Tajavour","TAJ","2345")
    spark.sql(f"INSERT INTO delta.`{table_path}` VALUES ({value})")
    
    # data = [("danial", "simes","456 north","Coimbatore","CBE","379"),("micheal", "fernandas","456 north","ukraine","uk","379")]
    # new_Data = spark.createDataFrame(data,["_c0", "_c1","_c2","_c3","_c4","_c5"])
    # new_Data.write.format('delta').mode('append').save(table_path)
    
def update_data():
    # spark.sql(f"UPDATE delta.`{table_path}` SET _c0='Update' WHERE _c0='value1'")
    
    data = [("danial", "simes","456 north","Coimbatore","CBE","789")]
    updates = spark.createDataFrame(data,["_c0", "_c1","_c2","_c3","_c4","_c5"])
    deltaTable.alias("et").merge(
        updates.alias("new"),
        'et._c5 = "789"'
        ).whenMatchedUpdateAll().execute()

def delete_data():
    spark.sql(f"DELETE FROM delta.`{table_path}` where lower(_c0)='sai'")
    # spark.sql(f"DELETE FROM delta.`{table_path}` where _c0 LIKE '%John%'")
    # spark.sql(f"DELETE FROM delta.`{table_path}` where lower(_c0) LIKE '%john%'") for case insensitive
    # deltaTable.delete("_c0 is null or _c0='danial'")

def upsert_data():
    # data = [("sai","pratap","1k1","Coimbatore","CBE","489")]
    # new_df = spark.createDataFrame(data,["_c0","_c1","_c2","_c3","_c4","_c5"])
    # deltaTable.alias("et").merge(
    #     new_df.alias("nd"),
    #     condition = 'et._c1="sai"'
    #     ).whenMatchedUpdate(
    #         set={
    #                 "et._c0":"nd._c0",
    #                 "et._c1":"nd._c1"
    #         }
    #     ).whenNotMatchedInsertAll().execute()
    data = ("sai","prushoth","1k1","Coimbatore","CBE","111")
    result = spark.sql(f"""
    MERGE INTO delta.`{table_path}` as t
    USING(SELECT '{data[0]}' as _c0, '{data[1]}' as _c1,'{data[2]}' as _c2, '{data[3]}' as _c3,'{data[4]}' as _c4, '{data[5]}' as _c5) as s
    ON t._c0 = 'asd'
    WHEN MATCHED THEN UPDATE SET t._c5=s._c5
    WHEN NOT MATCHED THEN INSERT (_c0,_c1,_c2,_c3,_c4,_c5) VALUES (s._c0,s._c1,s._c2,s._c3,s._c4,s._c5)
    """)

def select_data():
    # Using sql
    result = spark.sql(f'select * from delta.`{table_path}`')
    # print(result.show())
    
    #Using script
    df = spark.read.format("delta").load(table_path)
    print(df.show())
    
# def version():
    # df = spark.read.format("delta").option("versionAsOf", "5").load(table_path)
    # print(df.show())
    # df_Arr = df.collect()
    # for i in df_Arr:
    #     print(i)
        
def vacuum():
    deltaTable.vacuum(0)

if __name__ == '__main__':
    # create_table()
    insert_data()
    # update_data()
    # delete_data()
    # upsert_data()
    # select_data()
    vacuum()
    # version()
    
    
    
job.commit()