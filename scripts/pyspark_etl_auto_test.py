from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import datetime
import time

#old
# spark = SparkSession.builder \
#     .config('spark.jars.packages','com.datastax.spark:spark-cassandra-connector_2.12:3.1.0') \
#     .getOrCreate()

spark = SparkSession.builder \
    .config('spark.jars.packages','com.datastax.spark:spark-cassandra-connector_2.12:3.5.1') \
    .getOrCreate()

def cal_click(df):
    #cal click data
    click_data = df.filter(col("custom_track") == 'click')
    click_data = click_data.na.fill({'bid':0})
    click_data = click_data.na.fill({'job_id':0})
    click_data = click_data.na.fill({'publisher_id':0})
    click_data = click_data.na.fill({'group_id':0})
    click_data = click_data.na.fill({'campaign_id':0})
    click_data.createOrReplaceTempView('clickdata')
    click_df = spark.sql("""select date(ts) as date,hour(ts) as hour ,job_id,publisher_id,campaign_id,group_id,round(avg(bid),2) as bid_set , sum(bid) as spend_hour , count(*) as click from clickdata group by date,
    hour,job_id,publisher_id,campaign_id,group_id """)
    #click_df.show()
    return click_df

def cal_conversion(df):
    #cal conversion
    conversion_data = df.filter(col("custom_track") == 'conversion')
    conversion_data = conversion_data.na.fill({'job_id':0})
    conversion_data = conversion_data.na.fill({'publisher_id':0})
    conversion_data = conversion_data.na.fill({'group_id':0})
    conversion_data = conversion_data.na.fill({'campaign_id':0})
    conversion_data.createOrReplaceTempView('conversion_data')
    conversion_df = spark.sql("""select date(ts) as date,hour(ts) as hour ,job_id,publisher_id,campaign_id,group_id, count(*) as conversion from conversion_data group by date,
    hour,job_id,publisher_id,campaign_id,group_id """)
    #conversion_df.show()
    return conversion_df


def cal_qualified(df):
    #cal qualified
    qualified_data = df.filter(col("custom_track") == 'qualified')
    qualified_data = qualified_data.na.fill({'job_id':0})
    qualified_data = qualified_data.na.fill({'publisher_id':0})
    qualified_data = qualified_data.na.fill({'group_id':0})
    qualified_data = qualified_data.na.fill({'campaign_id':0})
    qualified_data.createOrReplaceTempView('qualified_data')
    qualified_df = spark.sql("""select date(ts) as date,hour(ts) as hour ,job_id,publisher_id,campaign_id,group_id, count(*) as qualified_application from qualified_data group by date,
    hour,job_id,publisher_id,campaign_id,group_id """)
    #qualified_df.show()
    return qualified_df


def cal_unqualified(df):
    #cal unqualified_application
    unqualified_data = df.filter(col("custom_track") == 'unqualified')
    unqualified_data = unqualified_data.na.fill({'job_id':0})
    unqualified_data = unqualified_data.na.fill({'publisher_id':0})
    unqualified_data = unqualified_data.na.fill({'group_id':0})
    unqualified_data = unqualified_data.na.fill({'campaign_id':0})
    unqualified_data.createOrReplaceTempView('unqualified_data')
    unqualified_df = spark.sql("""select date(ts) as date,hour(ts) as hour ,job_id,publisher_id,campaign_id,group_id, count(*) as disqualified_application from unqualified_data group by date,
    hour,job_id,publisher_id,campaign_id,group_id """)
    #unqualified_df.show()
    return unqualified_df

def process_final_data(click_df,conversion_df,qualifield_df,unqualified_df):
    #full join de tao combined output
    final_data = click_df.join(conversion_df,['job_id','date','hour','publisher_id','campaign_id','group_id'],'full').\
        join(qualifield_df,['job_id','date','hour','publisher_id','campaign_id','group_id'],"full").\
            join(unqualified_df,['job_id','date','hour','publisher_id','campaign_id','group_id'],"full")   
    #final_data.show()
    return final_data

def combined_data(df):
    click_output = cal_click(df)
    conversion_output = cal_conversion(df)
    qualified_output = cal_qualified(df)
    unqualified_output = cal_unqualified(df)
    combined_data = process_final_data(click_output,conversion_output,qualified_output,unqualified_output)
    #combined_data.show()
    return combined_data


def get_company_id(df):
    job_table = spark.read \
        .format("jdbc") \
        .option("driver","com.mysql.cj.jdbc.Driver") \
        .option("url", "jdbc:mysql://mysql:3307/lamtt14") \
        .option("dbtable", "job") \
        .option("user", "root") \
        .option("password", "1") \
        .load()
    job_table = job_table.select("id","group_id","campaign_id","company_id").withColumnRenamed("id","job_id")
    #job_table.show()
    final_df = df.join(job_table,["job_id","group_id","campaign_id"],"left")
    final_df = final_df.withColumn("Source",lit("Cassandra"))
    return final_df

def import_to_mySQL(df):
    #write to data warehouse mysql
    df.write  \
        .format("jdbc") \
        .option("driver","com.mysql.cj.jdbc.Driver") \
        .option("url", "jdbc:mysql://mysql:3307/lamtt14") \
        .option("dbtable", "events") \
        .option("user", "root") \
        .option("password", "1") \
        .mode("append")\
        .save()
    print("Data imported successfully")

def get_mysql_latest_time():    
    #sql = """(select max(Last_update_time) from events) data"""
    mysql_time = spark.read.format('jdbc')\
                            .option("driver","com.mysql.cj.jdbc.Driver") \
                            .option("url", "jdbc:mysql://mysql:3307/lamtt14") \
                            .option("user", "root") \
                            .option("password", "1") \
                            .option("query","select max(Last_update_time) from events") \
                            .load()
    
    mysql_time = mysql_time.take(1)[0][0]
    if mysql_time is None:
        mysql_latest = '1998-01-01 23:59:59'
    else :
        
        mysql_latest = mysql_time
    return mysql_latest    

def main(mysql_time):
    spark_df = spark.read.format("org.apache.spark.sql.cassandra") \
        .options(table='tracking',keyspace='lamtt14') \
        .load() \
        .where(col('ts') > mysql_time)
    #spark_df.show()

    df = spark_df.select("job_id","ts","custom_track","group_id","campaign_id","publisher_id","bid")
    df = df.filter((col("job_id").isNotNull()) & (col("custom_track").isNotNull()))
    #df.show()

    # tìm last_update_time = lấy max của cột ts
    last_update_time = df.select(max("ts")).collect()[0][0]

    #etl dữ liệu từ cassandra
    cassandra_output = combined_data(df)

    #join với company_id từ bảng job trong mysql
    final_output = get_company_id(cassandra_output)
    final_output = final_output.withColumn("Last_update_time",lit(last_update_time))
    final_output.show()
    
    # ghi dữ liệu vào mysql
    import_to_mySQL(final_output)
    return print('Task Finished')


if __name__ == "__main__":
    
    spark = SparkSession.builder \
            .getOrCreate()
    
    mysql_time = get_mysql_latest_time()
    main(mysql_time)
    
    spark.stop()
    









###
# def get_latest_time_cassandra():
#     data = spark.read.format("org.apache.spark.sql.cassandra").options(table='tracking',keyspace='lamtt14').load()
#     cassandra_latest_time = data.agg({'ts':'max'}).take(1)[0][0]
#     return cassandra_latest_time



# while True :
#     start_time = datetime.datetime.now()
#     cassandra_time = get_latest_time_cassandra()
#     print('Cassandra latest time is {}'.format(cassandra_time))
#     mysql_time = get_mysql_latest_time()
#     print('MySQL latest time is {}'.format(mysql_time))
#     if cassandra_time > mysql_time :
#         print("Found new data") 
#         main(mysql_time)
#     else :
#         print("No new data found")
#     end_time = datetime.datetime.now()
#     execution_time = (end_time - start_time).total_seconds()
#     print('Job takes {} seconds to execute'.format(execution_time))
#     time.sleep(10)

###