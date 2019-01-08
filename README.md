# spark-slowly-changing-dimention
Spark implementation of slowly changing dimention

Implemented a slowly changing dimention type 2 using Scala Spark and Pyspark.

After every run, save the updated data to Hive table in ORC format with Snappy compression.

Hive table:
Scala Application output Table   - yelp_data_scala_sbhange.yelp_user_hist

Table Directory                  - /user/sahilbhange/scala/yelp_hist/

Pyspark Application output Table - yelp_data_spark_df_sbhange.yelp_user_hist

Table Directory                  - /user/sahilbhange/spark_df/
