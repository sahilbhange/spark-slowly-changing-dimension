
package scd

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.{SparkConf, SparkContext}

import org.apache.hadoop.fs._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

import scala.io.Source
import java.util.Calendar
import java.text.SimpleDateFormat

object YelpUserScdMonthlyRun {
  def main(args: Array[String]): Unit = {

    println("SCD monthly run started")

    val proc_dt = args(0)

    val inputFormat = new SimpleDateFormat("yyyyMMdd")
    val reqFormat = new SimpleDateFormat("yyyy-MM-dd")

    // Date function to calculate the Expiry date value using Calender module

    val dateFun = Calendar.getInstance()

    // Convert the effective and expiry date in yyyy-MM-dd format

    val eff_dt = reqFormat.format(inputFormat.parse(proc_dt))

    dateFun.setTime(inputFormat.parse(proc_dt))
    dateFun.add(Calendar.DATE, -1)

    val exp_dt = reqFormat.format(dateFun.getTime())

    val stgPath = new Path("/user/sahilbhange/scala/yelp_hist_stg/")
    val histPath = new Path("/user/sahilbhange/scala/yelp_hist/")

   // Define the STG and HIST data directory
    val histTabPath = "/user/sahilbhange/scala/yelp_hist/"
    val stgTabPath = "/user/sahilbhange/scala/yelp_hist_stg/"

    val input_file = s"/user/sahilbhange/data/yelp_user_$proc_dt.csv"

    val conf = new SparkConf().setAppName("Yelp-user-scd-2-monthly-run").setMaster("yarn-client")

    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    val fs = FileSystem.get(sc.hadoopConfiguration)

    // Set the Spark shuffle and ORC parameters for better performance
    sqlContext.setConf("spark.sql.shuffle.partitions", "10")
    sqlContext.setConf("spark.sql.orc.enabled","true")

    import sqlContext.implicits._

    if (!fs.exists(stgPath)) {
      println("Staging directory does not exist, create STG directory")
      fs.mkdirs(stgPath)

    } else {
            println("Staging directory exist")
            }

    // Load new file data to STG
    val yelp_data_stg = sqlContext.read.format("com.databricks.spark.csv").
                                    option("header", "true").
                                    option("inferSchema", "true").
                                    option("quote", "\"").
                                    option("ignoreLeadingWhiteSpace", true).
                                    load(input_file)

    // Load historical yelp user data to HIST
    val yelp_data_hist = sqlContext.read.format("org.apache.spark.sql.execution.datasources.orc").
                                    load(histTabPath)


    // Get only expired records from the HIST table
    val yelp_data_expired = yelp_data_hist.as("hist").filter($"exp_dt"=!="2099-12-31").select($"hist.*")

    // Get records which are active and not changed recently i.e. user with same values from STG and HIST
    val yelp_data_intersect = yelp_data_hist.as("hist").filter($"exp_dt"==="2099-12-31").
                                        join(yelp_data_stg.as("stg"), Seq("user_id"), "inner").
                                        where($"hist.exp_dt" === "2099-12-31" && $"hist.review_count" === $"stg.review_count"
                                              && $"hist.useful" === $"stg.useful" && $"hist.funny" === $"stg.funny"
                                              && $"hist.cool" === $"stg.cool" && $"hist.fans" === $"stg.fans"
                                              && $"hist.average_stars" === $"stg.average_stars"
                                              && $"hist.compliment_hot" === $"stg.compliment_hot"
                                              && $"hist.compliment_more" === $"stg.compliment_more"
                                              && $"hist.compliment_profile" === $"stg.compliment_profile"
                                              && $"hist.compliment_cute" === $"stg.compliment_cute"
                                              && $"hist.compliment_list" === $"stg.compliment_list"
                                              && $"hist.compliment_cool" === $"stg.compliment_cool"
                                              && $"hist.compliment_funny" === $"stg.compliment_funny"
                                              && $"hist.compliment_writer" === $"stg.compliment_writer"
                                              && $"hist.compliment_photos" === $"stg.compliment_photos").select($"hist.*")

    // Get new user_id and recently updated user_id from the STG table and set effective and expiry date as (current day minus 1)
    val yelp_data_upnew = yelp_data_stg.as("stg").
                                        join(yelp_data_hist.as("hist").filter($"exp_dt"==="2099-12-31"), Seq("user_id"), "left").
                                        where($"hist.user_id".isNull || $"hist.review_count" =!= $"stg.review_count" ||
                                                $"hist.useful" =!= $"stg.useful" || $"hist.funny" =!= $"stg.funny" ||
                                                $"hist.cool" =!= $"stg.cool" || $"hist.fans" =!= $"stg.fans" ||
                                                $"hist.average_stars" =!= $"stg.average_stars" ||
                                                $"hist.compliment_hot" =!= $"stg.compliment_hot" ||
                                                $"hist.compliment_more" =!= $"stg.compliment_more" ||
                                                $"hist.compliment_profile" =!= $"stg.compliment_profile" ||
                                                $"hist.compliment_cute" =!= $"stg.compliment_cute" ||
                                                $"hist.compliment_list" =!= $"stg.compliment_list" ||
                                                $"hist.compliment_cool" =!= $"stg.compliment_cool" ||
                                                $"hist.compliment_funny" =!= $"stg.compliment_funny" ||
                                                $"hist.compliment_writer" =!= $"stg.compliment_writer" ||
                                                $"hist.compliment_photos" =!= $"stg.compliment_photos").select($"stg.*")

    val yelp_data_upnew_date = yelp_data_upnew.withColumn("eff_dt",to_date(lit(eff_dt), "yyyy-MM-dd")).
                                        withColumn("exp_dt",to_date(lit("2099-12-31"),"yyyy-MM-dd"))

    // Get old updated user_id from the HIST table and set expiry date i.e. (2099-12-31)
    val yelp_data_upexp = yelp_data_hist.as("hist").filter($"exp_dt"==="2099-12-31").
                                        join(yelp_data_stg.as("stg"), Seq("user_id"), "left").
                                        where($"stg.user_id".isNull || $"hist.review_count" =!= $"stg.review_count" ||
                                                $"hist.useful" =!= $"stg.useful" || $"hist.funny" =!= $"stg.funny" ||
                                                $"hist.cool" =!= $"stg.cool" || $"hist.fans" =!= $"stg.fans" ||
                                                $"hist.average_stars" =!= $"stg.average_stars" ||
                                                $"hist.compliment_hot" =!= $"stg.compliment_hot" ||
                                                $"hist.compliment_more" =!= $"stg.compliment_more" ||
                                                $"hist.compliment_profile" =!= $"stg.compliment_profile" ||
                                                $"hist.compliment_cute" =!= $"stg.compliment_cute" ||
                                                $"hist.compliment_list" =!= $"stg.compliment_list" ||
                                                $"hist.compliment_cool" =!= $"stg.compliment_cool" ||
                                                $"hist.compliment_funny" =!= $"stg.compliment_funny" ||
                                                $"hist.compliment_writer" =!= $"stg.compliment_writer" ||
                                                $"hist.compliment_photos" =!= $"stg.compliment_photos").select($"hist.*")


    val yelp_data_upexp_date = yelp_data_upexp.withColumn("exp_dt",to_date(lit(exp_dt),"yyyy-MM-dd"))


    // Merge data from all the above dataframes to create historical table data

    val yelp_data_all = yelp_data_intersect.unionAll(yelp_data_expired).unionAll(yelp_data_upnew_date).
                                                                        unionAll(yelp_data_upexp_date)


    // Write updated historical data to temperory STG path
    yelp_data_all.coalesce(2).write.mode(SaveMode.Overwrite).format("orc").save(stgTabPath)

    // Delete the existing HIST data and load new data from STG to HIST
    if (fs.exists(histPath)) {
      println("History table directory exist, deleting the old table data")
      fs.delete(histPath)

    } else {
            println("History table directory does not exist")
            }

    if (fs.exists(stgPath)) {
      println("Staging directory is loaded with updated data, ")
      fs.rename(stgPath,histPath)

    } else {
            println("Staging directory exist")
            }

    println("Monthly run completed successfully")
  }

}
 
