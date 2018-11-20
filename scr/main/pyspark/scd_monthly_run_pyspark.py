
#spark-submit --master yarn --conf spark.ui.port=12789 --num-executors 4 --executor-memory 2GB  /home/sahilbhange/hiveql/src/pyspark/scd_monthly_
run_pyspark.py '20180831'

from pyspark.context import SparkContext,SparkConf
from pyspark.sql import HiveContext
from datetime import datetime, timedelta

import sys

conf=SparkConf().setAppName("SCD-2-monthly-spark").setMaster("yarn-client")

sc = SparkContext(conf=conf)

sqlContext = HiveContext(sc)

run_date = sys.argv[1]

eff_dt = datetime.strptime(run_date, '%Y%m%d').strftime('%Y-%m-%d')

exp_dt = (datetime.strptime(run_date,'%Y%m%d')+ timedelta(days=-1)).strftime('%Y-%m-%d')

input_file = "/user/sahilbhange/data/yelp_user_{}.csv".format(run_date)

yelp_data_stg = sqlContext.read.format('com.databricks.spark.csv').options(header='true',  inferschema='true').load(input_file).coalesce(2)

yelp_data_stg.registerTempTable("yelp_user_stg")

yelp_data_hist_temp = sqlContext.sql(""" select * from yelp_data_spark_sbhange.yelp_user_hist """).coalesce(2)

yelp_data_hist_temp.registerTempTable("yelp_user_hist")

yelp_data_hist = sqlContext.sql("""select * from yelp_user_hist where exp_dt != '2099-12-31'

union all

select hist.* from yelp_user_hist hist
inner join yelp_user_stg stg
on hist.user_id = stg.user_id
where hist.review_count  = stg.review_count
and hist.useful = stg.useful
and hist.funny = stg.funny
and hist.cool = stg.cool
and hist.fans = stg.fans
and hist.average_stars = float(stg.average_stars)
and hist.compliment_hot = stg.compliment_hot
and hist.compliment_more = stg.compliment_more
and hist.compliment_profile = stg.compliment_profile
and hist.compliment_cute = stg.compliment_cute
and hist.compliment_list = stg.compliment_list
and hist.compliment_note = stg.compliment_note
and hist.compliment_plain = stg.compliment_plain
and hist.compliment_cool = stg.compliment_cool
and hist.compliment_funny = stg.compliment_funny
and hist.compliment_writer = stg.compliment_writer
and hist.compliment_photos = stg.compliment_photos
and hist.exp_dt = '2099-12-31'

union all

select stg.user_id,  stg.name,  stg.review_count,  stg.yelping_since,  stg.useful,  stg.funny,
stg.cool,  stg.fans,  stg.elite,  stg.average_stars,  stg.compliment_hot,  stg.compliment_more,
stg.compliment_profile,  stg.compliment_cute,  stg.compliment_list,  stg.compliment_note,
stg.compliment_plain,  stg.compliment_cool,  stg.compliment_funny,  stg.compliment_writer,
stg.compliment_photos, cast(date_format('{}','yyyy-MM-dd') as date),
cast(date_format('2099-12-31','yyyy-MM-dd') as date), stg.date
from yelp_user_stg stg
left join (select * from yelp_user_hist where exp_dt = '2099-12-31') hist
on hist.user_id = stg.user_id
where hist.user_id is null
or hist.review_count  != stg.review_count
or hist.useful != stg.useful
or hist.funny != stg.funny
or hist.cool != stg.cool
or hist.fans != stg.fans
or hist.average_stars != float(stg.average_stars)
or hist.compliment_hot != stg.compliment_hot
or hist.compliment_more != stg.compliment_more
or hist.compliment_profile != stg.compliment_profile
or hist.compliment_cute != stg.compliment_cute
or hist.compliment_list != stg.compliment_list
or hist.compliment_note != stg.compliment_note
or hist.compliment_plain != stg.compliment_plain
or hist.compliment_cool != stg.compliment_cool
or hist.compliment_funny != stg.compliment_funny
or hist.compliment_writer != stg.compliment_writer
or hist.compliment_photos != stg.compliment_photos

union all

select hist.user_id,  hist.name,  hist.review_count,  hist.yelping_since,  hist.useful,  hist.funny,
hist.cool,  hist.fans,  hist.elite,  hist.average_stars,  hist.compliment_hot,  hist.compliment_more,
hist.compliment_profile,  hist.compliment_cute,  hist.compliment_list,  hist.compliment_note,
hist.compliment_plain,  hist.compliment_cool,  hist.compliment_funny,  hist.compliment_writer,
hist.compliment_photos, hist.eff_dt, cast(date_format('{}','yyyy-MM-dd') as date), hist.proc_dt
from (select * from yelp_user_hist where exp_dt = '2099-12-31') hist
left join yelp_user_stg stg
on hist.user_id = stg.user_id
where stg.user_id is null
or hist.name != stg.name
or hist.review_count  != stg.review_count
or hist.useful != stg.useful
or hist.funny != stg.funny
or hist.cool != stg.cool
or hist.fans != stg.fans
or hist.average_stars != float(stg.average_stars)
or hist.compliment_hot != stg.compliment_hot
or hist.compliment_more != stg.compliment_more
or hist.compliment_profile != stg.compliment_profile
or hist.compliment_cute != stg.compliment_cute
or hist.compliment_list != stg.compliment_list
or hist.compliment_note != stg.compliment_note
or hist.compliment_plain != stg.compliment_plain
or hist.compliment_cool != stg.compliment_cool
or hist.compliment_funny != stg.compliment_funny
or hist.compliment_writer != stg.compliment_writer
or hist.compliment_photos != stg.compliment_photos""".format(eff_dt,exp_dt))

yelp_data_hist.write.mode("Overwrite").insertInto("yelp_data_spark_sbhange.yelp_user_hist")
