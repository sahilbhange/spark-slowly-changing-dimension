
# spark-submit --master yarn --conf spark.ui.port=12789 --num-executors 2 --executor-memory 1GB  --packages com.databricks:spark-csv_2.10:1.4.0 /
home/sahilbhange/hiveql/src/pyspark/scd_first_run_pyspark.py '20180731'

from pyspark.context import SparkContext,SparkConf
from pyspark.sql import HiveContext
from datetime import datetime, timedelta

import sys

conf=SparkConf().setAppName("SCD-2-spark").setMaster("yarn-client")

sc = SparkContext(conf=conf)

sqlContext = HiveContext(sc)

run_date = sys.argv[1]

eff_dt = datetime.strptime(run_date, '%Y%m%d').strftime('%Y-%m-%d')

exp_dt = (datetime.strptime(run_date,'%Y%m%d')+ timedelta(days=-1)).strftime('%Y-%m-%d')

input_file = "/user/sahilbhange/data/yelp_user_{}.csv".format(run_date)

yelp_data = sqlContext.read.format('com.databricks.spark.csv').options(header='true',  inferschema='true').load(input_file).coalesce(2)

yelp_data.registerTempTable("yelp_data_table")

yelp_data_hist_df = sqlContext.sql("""select user_id,  name,  review_count,  date(yelping_since),  useful,  funny,  cool,
                                   fans,  elite,  float(average_stars),  compliment_hot,  compliment_more,  compliment_profile,
                                   compliment_cute,  compliment_list,  compliment_note,  compliment_plain,  compliment_cool,
                                   compliment_funny,  compliment_writer,  compliment_photos, cast(date_format('{}','yyyy-MM-dd') as date),
                                   cast(date_format('2099-12-31','yyyy-MM-dd') as date), date from yelp_data_table""".format(eff_dt))

yelp_data_hist_df.write.mode("Overwrite").insertInto("yelp_data_spark_sbhange.yelp_user_hist")
