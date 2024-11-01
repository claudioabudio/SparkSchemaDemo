from pyspark.sql import SparkSession
from lib.logger import Log4J
from pyspark.sql.types import StructType, StructField, DateType, StringType, IntegerType

if __name__ == "__main__":
    spark = SparkSession.builder \
            .master("local[3]") \
            .appName("SparkSchemaDemo") \
            .getOrCreate()
    
    logger = Log4J(spark)
    flight_Time_CSV = spark.read \
        .format("csv") \
        .option("header", "true") \
        .option("inferSchema", True) \
        .load("data/flight*.csv")
    
    flight_Time_CSV.show(10)
    logger.info(f"CSV Schema: ${flight_Time_CSV.schema.simpleString()}")
    
    flight_Time_Json = spark.read \
        .format("json") \
        .load("data/flight*.json")
    
    flight_Time_Json.show(10)
    logger.info(f"JSON Schema: ${flight_Time_Json.schema.simpleString()}")

    flight_time_parquet = spark.read \
        .format("parquet") \
        .load("data/flight*.parquet")
    
    flight_time_parquet.show(10)
    logger.info(f"Parquet Schema: ${flight_time_parquet.schema.simpleString()}")


    # defining a schema in spark Programmatically
    flight_schema_struct = StructType([ 
        StructField("FL_DATE", DateType()),
        StructField("OP_CARRIER", StringType()),
        StructField("OP_CARRIER_FL_NUM", IntegerType()),
        StructField("ORIGIN", StringType()),
        StructField("ORIGIN_CITY_NAME", StringType()),
        StructField("DEST", StringType()),
        StructField("DEST_CITY_NAME", StringType()),
        StructField("CRS_DEP_TIME", IntegerType()),
        StructField("DEP_TIME", IntegerType()),
        StructField("WHEELS_ON", IntegerType()),
        StructField("TAXI_IN", IntegerType()),
        StructField("CRS_ARR_TIME", IntegerType()),
        StructField("ARR_TIME", IntegerType()),
        StructField("CANCELLED", IntegerType()),
        StructField("DISTANCE", IntegerType())
    ])

    flight_Time_csv = spark.read \
        .format("csv") \
        .option("header", "true") \
        .schema(flight_schema_struct) \
        .option("mode", "FAILFAST") \
        .option("dateFormat", "M/d/yyyy") \
        .load("data/flight*.csv")
    
    flight_Time_csv.show(10)
    logger.info(f"CSV Schema: ${flight_Time_csv.schema.simpleString()}")

    #Defining the schema in a DDL string 
    flight_schema_ddl = """FL_DATE DATE, OP_CARRIER STRING, OP_CARRIER_FL_NUM INT, ORIGIN STRING, 
        ORIGIN_CITY_NAME STRING, DEST STRING, DEST_CITY_NAME STRING, CRS_DEP_TIME INT, DEP_TIME INT,
        WHEELS_ON INT, TAXI_IN INT, CRS_ARR_TIME INT, ARR_TIME INT, CANCELLED INT, DISTANCE INT"""
    
    flight_Time_Json = spark.read \
        .format("json") \
        .schema(flight_schema_ddl) \
        .option("mode", "FAILFAST") \
        .option("dateFormat", "M/d/yyyy") \
        .load("data/flight*.json")
    
    flight_Time_Json.show(10)
    logger.info(f"JSON Schema: ${flight_Time_Json.schema.simpleString()}")
