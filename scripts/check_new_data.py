import pyspark.sql.functions as sf
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from datetime import datetime


def get_latest_time_cassandra(spark):
    data = spark.read.format("org.apache.spark.sql.cassandra") \
                     .options(table='tracking', keyspace='cycling') \
                     .load()
    cassandra_latest_time = data.agg({'ts': 'max'}).take(1)[0][0]
    return cassandra_latest_time

def get_mysql_latest_time(spark):
    mysql_time = spark.read.format('jdbc')\
                            .option("driver","com.mysql.cj.jdbc.Driver") \
                            .option("url", "jdbc:mysql://mysql:3308/lamtt14") \
                            .option("user", "root") \
                            .option("password", "root") \
                            .option("query","select max(Last_update_time) from events") \
                            .load()
    
    mysql_time = mysql_time.take(1)[0][0]
    if mysql_time is None:
        #mysql_latest = '1998-01-01 23:59:59'
        mysql_latest = datetime(1998, 1, 1, 23, 59, 59)
    else :
        
        mysql_latest = mysql_time
    return mysql_latest

def main():
    spark = SparkSession.builder \
        .config("spark.cassandra.connection.host", "cassandra") \
        .config("spark.cassandra.connection.port", "9044") \
        .config('spark.jars.packages','com.datastax.spark:spark-cassandra-connector_2.12:3.5.1') \
        .getOrCreate()

    try:
        cassandra_time = get_latest_time_cassandra(spark)
        print(f'Cassandra latest time is {cassandra_time}')

        mysql_time = get_mysql_latest_time(spark)
        print(f'MySQL latest time is {mysql_time}')

        if cassandra_time > mysql_time:
            print('Have new data, continue task')
            print('NEW_DATA=True')
        else:
            print('No new data found')
            print('NEW_DATA=False')

    finally:
        spark.stop()

if __name__ == "__main__":
    main()
