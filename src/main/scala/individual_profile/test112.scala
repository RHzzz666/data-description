package individual_profile

import org.apache.spark.sql.execution.datasources.hbase.HBaseTableCatalog
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession, functions}

object test112 {
  def main(args: Array[String]): Unit = {
    def user_log_catalog =
      s"""{
         |"table":{"namespace":"default", "name":"tbl_logs"},
         |"rowkey":"id",
         |"columns":{
         |"id":{"cf":"rowkey", "col":"id", "type":"string"},
         |"global_user_id":{"cf":"cf", "col":"global_user_id", "type":"string"},
         |"user_agent":{"cf":"cf", "col":"user_agent", "type":"string"},
         |"loc_url":{"cf":"cf", "col":"loc_url", "type":"string"},
         |"log_time":{"cf":"cf", "col":"log_time", "type":"string"}
         |}
         |}""".stripMargin

    val spark = SparkSession.builder()
      .appName("behavior_record")
      .master("local")
      .getOrCreate()

    val log_df: DataFrame = spark.read
      .option(HBaseTableCatalog.tableCatalog, user_log_catalog)
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .load()

    import spark.implicits._
    val get_id = functions.udf(string_last_3_char _)
    val log_pre_df = log_df.drop('id) 
      .select('global_user_id,'user_agent,'loc_url,'log_time,
        when('loc_url like "%login%" ,"登录页")
          .when('loc_url like "%order%","我的订单页")
          .when('loc_url like "%product%","商品页")
          .when('loc_url like "%item%","分类页")
          .when('loc_url like "%index%","首页")
          .otherwise("其他页面")
          .as("scanned_page"),
        when(hour('log_time) between(1,7),"1点-7点")
          .when(hour('log_time) between(8,12),"8点-12点")
          .when(hour('log_time) between(13,17),"13点-17点")
          .when(hour('log_time) between(18,21),"18点-21点")
          .when(hour('log_time) between(22,24),"22点-24点")
          .as("log_time_arrange"),
        when('user_agent like("%Android%"),"Android")
          .when('user_agent like("%iPhone%"),"IOS")
          .when('user_agent like("%WOW%"),"Window")
          .when('user_agent like("%Linux%"),"Linux")
          .otherwise("Mac")
          .as("device_type")

      )
      .withColumn("pre_time", lag('log_time,1) over Window.partitionBy('global_user_id).orderBy('log_time.desc))
      .withColumn("scan_time",when(isnull('pre_time),"0").when((unix_timestamp('pre_time)-unix_timestamp('log_time))<60,"1分钟内")
        .when((unix_timestamp('pre_time)-unix_timestamp('log_time)) between(60,300),"1-5分钟")
        .when((unix_timestamp('pre_time)-unix_timestamp('log_time))>300,"5分钟以上"))
    //访问频率，目前暂无完全合并的需求
    val log_scan = log_pre_df.groupBy('global_user_id,'scanned_page).agg(datediff(max('log_time),min('log_time)) as "scan_total_time",count('scanned_page) as "count_scan")
      .withColumn("log_frequency",when('count_scan/'scan_total_time>1,"经常").
        when('count_scan/'scan_total_time between(1,1.5),"很少").
        when('count_scan/'scan_total_time between(0,1),"偶尔")
        when('count_scan/'scan_total_time===0,"从不"))
    // .groupBy('global_user_id).agg(collect_set('scanned_page) as("scanned_page"),collect_set('访问频率) as("'访问频率"))

    //
    log_scan.show(false)
    //+--------------+------------+---------------+----------+-------------+
    //|global_user_id|scanned_page|scan_total_time|count_scan|log_frequency|
    //+--------------+------------+---------------+----------+-------------+
    //|149           |登录页      |22             |10        |偶尔         |
    //|162           |首页        |27             |7         |偶尔         |
    val logDF = log_pre_df.groupBy('global_user_id).agg(max('log_time) as "log_time",
      sum(when('scanned_page==="登录页",1).otherwise(0)) as "count_log")

    logDF.show(false)
    val res = log_scan.join(logDF,log_scan.col("global_user_id")===logDF.col("global_user_id"))
      .drop(logDF.col("global_user_id"))
    //
    //+--------------+-------------------+---------+
    //|global_user_id|log_time           |count_log|
    //+--------------+-------------------+---------+
    //|296           |2019-08-17 21:33:05|6        |
    //|467           |2019-08-17 22:40:36|11       |
    def behavior_record_write=
      s"""{
         |"table":{"namespace":"default", "name":"user_behavior_record"},
         |"rowkey":"global_user_id",
         |"columns":{
         |"global_user_id":{"cf":"rowkey", "col":"global_user_id", "type":"string"},
         |"scanned_page":{"cf":"cf", "col":"scanned_page", "type":"string"},
         |"scan_total_time":{"cf":"cf", "col":"scan_total_time", "type":"string"},
         |"count_scan":{"cf":"cf", "col":"count_scan", "type":"string"},
         |"log_frequency":{"cf":"cf", "col":"log_frequency", "type":"string"},
         |"log_time":{"cf":"cf", "col":"log_time", "type":"string"},
         |"count_log":{"cf":"cf", "col":"count_log", "type":"string"},
         |"log_time_arrange":{"cf":"cf", "col":"log_time_arrange", "type":"string"}
         |}
         |}""".stripMargin

    res.write
      .option(HBaseTableCatalog.tableCatalog, behavior_record_write)
      .option(HBaseTableCatalog.newTable, "5")
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .save()
  }
  def string_last_3_char(str: String): String = {
    val len = str.length
    if (len > 3) str.substring(len - 3, len)
    else str
  }
}
