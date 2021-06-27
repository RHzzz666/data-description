package gala2

import gala2.read.spark
import org.apache.spark.sql.execution.datasources.hbase.HBaseTableCatalog
import org.apache.spark.sql.functions.{col, count}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SQLContext, SparkSession}

import scala.util.parsing.json.{JSON, JSONArray, JSONObject}

object read {
  val spark = SparkSession.builder().master("local").appName("test").getOrCreate()
  import spark.implicits._
  def Init():Unit={
    var ata=getgender()
  }

  def test():String={
    def user_final_2 =
      s"""{
         |"table":{"namespace":"default","name":"user_final_2nd"},
         |"rowkey":"id",
         |"columns":{
         |"id":{"cf":"rowkey","col":"id","type":"string"},
         |"member_id":{"cf":"cf","col":"member_id","type":"string"},
         |"e_mail":{"cf":"cf","col":"email","type":"string"},
         |"user_name":{"cf":"cf","col":"username","type":"string"},
         |"password":{"cf":"cf","col":"password","type":"string"},
         |"birthday":{"cf":"cf","col":"birthday","type":"string"},
         |"mobile":{"cf":"cf","col":"mobile","type":"string"},
         |"money":{"cf":"cf","col":"money","type":"string"},
         |"money_pwd":{"cf":"cf","col":"money_pwd","type":"string"},
         |"nick_name":{"cf":"cf","col":"nick_name","type":"string"},
         |"last_login_time":{"cf":"cf","col":"last_login_time","type":"string"},
         |"register_time":{"cf":"cf","col":"register_time","type":"string"},
         |"qq":{"cf":"cf","col":"qq","type":"string"},
         |"job":{"cf":"cf","col":"job","type":"string"},
         |"political_face":{"cf":"cf","col":"political_face","type":"string"},
         |"age_class":{"cf":"cf","col":"age_class","type":"string"},
         |"nationality":{"cf":"cf","col":"nationality","type":"string"},
         |"marriage":{"cf":"cf","col":"marriage","type":"string"},
         |"is_in_blacklist":{"cf":"cf","col":"is_in_blacklist","type":"string"},
         |"constellation":{"cf":"cf","col":"constellation","type":"string"},
         |"region":{"cf":"cf","col":"region","type":"string"},
         |"gender":{"cf":"cf","col":"gender","type":"string"},

         |"scanned_goods":{"cf":"cf", "col":"scanned_goods", "type":"string"},
         |"product_type":{"cf":"cf", "col":"product_type", "type":"string"},
         |"good_bought":{"cf":"cf", "col":"good_bought", "type":"string"},
         |"brand_preference":{"cf":"cf", "col":"brand_preference", "type":"string"},
         |"product_name":{"cf":"cf", "col":"productName", "type":"string"},

         |"payment_way":{"cf":"cf", "col":"payment_way", "type":"string"},
         |"shopping_cycle":{"cf":"cf", "col":"shopping_cycle", "type":"string"},
         |"ave_price":{"cf":"cf", "col":"ave_price", "type":"string"},
         |"ave_price_range":{"cf":"cf", "col":"ave_price_range", "type":"string"},
         |"order_count":{"cf":"cf", "col":"order_count","type":"long"},
         |"order_highest":{"cf":"cf", "col":"order_highest", "type":"string"},
         |"order_highest_range":{"cf":"cf", "col":"order_highest_range", "type":"string"},
         |"frequency":{"cf":"cf", "col":"frequency", "type":"string"},
         |"frequency_range(高,中,低)":{"cf":"cf", "col":"frequency_range(高,中,低)", "type":"string"},
         |"exchange_item_rate":{"cf":"cf", "col":"exchange_item_rate", "type":"string"},
         |"return_item_rate":{"cf":"cf", "col":"return_item_rate", "type":"string"},
         |"exchange_item_rate(高,中,低)":{"cf":"cf", "col":"exchange_item_rate(高,中,低)", "type":"string"},
         |"return_item_rate(高,中,低)":{"cf":"cf", "col":"return_item_rate(高,中,低)", "type":"string"},
         |"consumption":{"cf":"cf", "col":"consumption", "type":"string"},

         |"scanned_page":{"cf":"cf", "col":"scanned_page", "type":"string"},
         |"scan_total_time":{"cf":"cf", "col":"scan_total_time", "type":"string"},
         |"count_scan":{"cf":"cf", "col":"count_scan", "type":"string"},
         |"log_frequency":{"cf":"cf", "col":"log_frequency", "type":"string"},
         |"log_time":{"cf":"cf", "col":"log_time", "type":"string"},
         |"count_log":{"cf":"cf", "col":"count_log", "type":"string"},
         |"log_time_arrange":{"cf":"cf", "col":"log_time_arrange", "type":"string"},
         |
         |"consumption_ablity":{"cf":"cf", "col":"consumption_ablity", "type":"string"}
         |}
         |}""".stripMargin
    val df= spark.read
      .option(HBaseTableCatalog.tableCatalog, user_final_2)
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .load().toDF()
    val s4=df.where(col("user_name")==="督咏")
    s4.toJSON.collectAsList().toString

  }

  def read_by_label(gender:String, birth:String, job:String, shopping_cycle:String, consumption_ablity:String): String =
  {
    def user_final_2 =
      s"""{
         |"table":{"namespace":"default","name":"user_final_2nd"},
         |"rowkey":"id",
         |"columns":{
         |"id":{"cf":"rowkey","col":"id","type":"string"},
         |"member_id":{"cf":"cf","col":"member_id","type":"string"},
         |"e_mail":{"cf":"cf","col":"email","type":"string"},
         |"user_name":{"cf":"cf","col":"username","type":"string"},
         |"password":{"cf":"cf","col":"password","type":"string"},
         |"birthday":{"cf":"cf","col":"birthday","type":"string"},
         |"mobile":{"cf":"cf","col":"mobile","type":"string"},
         |"money":{"cf":"cf","col":"money","type":"string"},
         |"money_pwd":{"cf":"cf","col":"money_pwd","type":"string"},
         |"nick_name":{"cf":"cf","col":"nick_name","type":"string"},
         |"last_login_time":{"cf":"cf","col":"last_login_time","type":"string"},
         |"register_time":{"cf":"cf","col":"register_time","type":"string"},
         |"qq":{"cf":"cf","col":"qq","type":"string"},
         |"job":{"cf":"cf","col":"job","type":"string"},
         |"political_face":{"cf":"cf","col":"political_face","type":"string"},
         |"age_class":{"cf":"cf","col":"age_class","type":"string"},
         |"nationality":{"cf":"cf","col":"nationality","type":"string"},
         |"marriage":{"cf":"cf","col":"marriage","type":"string"},
         |"is_in_blacklist":{"cf":"cf","col":"is_in_blacklist","type":"string"},
         |"constellation":{"cf":"cf","col":"constellation","type":"string"},
         |"region":{"cf":"cf","col":"region","type":"string"},
         |"gender":{"cf":"cf","col":"gender","type":"string"},

         |"scanned_goods":{"cf":"cf", "col":"scanned_goods", "type":"string"},
         |"product_type":{"cf":"cf", "col":"product_type", "type":"string"},
         |"good_bought":{"cf":"cf", "col":"good_bought", "type":"string"},
         |"brand_preference":{"cf":"cf", "col":"brand_preference", "type":"string"},
         |"product_name":{"cf":"cf", "col":"productName", "type":"string"},

         |"payment_way":{"cf":"cf", "col":"payment_way", "type":"string"},
         |"shopping_cycle":{"cf":"cf", "col":"shopping_cycle", "type":"string"},
         |"ave_price_range":{"cf":"cf", "col":"ave_price_range", "type":"string"},
         |"order_highest_range":{"cf":"cf", "col":"order_highest_range", "type":"string"},
         |"frequency_range":{"cf":"cf", "col":"frequency_range(高,中,低)", "type":"string"},
         |"exchange_item_rate":{"cf":"cf", "col":"exchange_item_rate(高,中,低)", "type":"string"},
         |"return_item_rate":{"cf":"cf", "col":"return_item_rate(高,中,低)", "type":"string"},
         |"consumption":{"cf":"cf", "col":"consumption", "type":"string"},

         |"scanned_page":{"cf":"cf", "col":"scanned_page", "type":"string"},
         |"log_frequency":{"cf":"cf", "col":"log_frequency", "type":"string"},
         |"log_time":{"cf":"cf", "col":"log_time", "type":"string"},
         |"log_time_arrange":{"cf":"cf", "col":"log_time_arrange", "type":"string"},
         |
         |"consumption_ablity":{"cf":"cf", "col":"consumption_ablity", "type":"string"}
         |}
         |}""".stripMargin
    var df= spark.read
      .option(HBaseTableCatalog.tableCatalog, user_final_2)
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .load().toDF()
    if(gender!="all"){
      df=df.where(col("gender")===gender)
    }
    if(birth!="all"){
      df=df.where(col("age_class")===birth)
    }
    if(job!="all"){
      df=df.where(col("job")===job)
    }
    if(shopping_cycle!="all"){
      df=df.where(col("shopping_cycle")===shopping_cycle)
    }
    if(consumption_ablity!="all"){
      df=df.where(col("consumption_ablity")===consumption_ablity)
    }
    df.toJSON.collectAsList().toString
  }

  def read_user_weekly(id:String):String=
  {
    def user_weekly=
      s"""{
         |  "table":{"namespace":"default", "name":"user_weekly"},
         |  "rowkey":"rowKey",
         |  "columns":{
         |    "Id":{"cf":"cf", "col":"Id", "type":"long"},
         |    "rowKey":{"cf":"rowkey", "col":"rowKey", "type":"long"},
         |    "finishWeek":{"cf":"cf", "col":"finishWeek", "type":"string"},
         |    "weeklyTime":{"cf":"cf", "col":"weeklyTime", "type":"string"},
         |    "year":{"cf":"cf", "col":"year", "type":"string"}
         |  }
         |}""".stripMargin
    val df= spark.read
      .option(HBaseTableCatalog.tableCatalog, user_weekly)
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .load().toDF()
    val s4=df.where(col("id")===id).drop(col("id"))
    s4.toJSON.collectAsList().toString
  }

  def read_user_class(name:String):String=
  {
    def user_class=
      s"""{
         |  "table":{"namespace":"default", "name":"user_class"},
         |  "rowkey":"id",
         |  "columns":{
         |    "id":{"cf":"rowkey", "col":"id", "type":"long"},
         |    "username":{"cf":"cf", "col":"username", "type":"string"},
         |    "consumptionAblity":{"cf":"cf", "col":"consumptionAblity", "type":"string"}
         |  }
         |}""".stripMargin
    val df= spark.read
      .option(HBaseTableCatalog.tableCatalog, user_class)
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .load().toDF()
    val s4=df.where(col("username")===name).drop(col("id"))
    s4.toJSON.collectAsList().toString
  }

  def read_orders_info(member_id: String):String=
  {
    def user_orders_info_simple =
      s"""{
         |"table":{"namespace":"default", "name":"user_orders_info_simple"},
         |"rowkey":"id",
         |"columns":{
         |"id":{"cf":"rowkey", "col":"id", "type":"string"},
         |"member_id":{"cf":"cf", "col":"member_id", "type":"string"},
         |"shopping_time":{"cf":"cf", "col":"shopping_time", "type":"string"},
         |"order_amount":{"cf":"cf", "col":"order_amount", "type":"string"}
         |}
         |}""".stripMargin
    val df= spark.read
      .option(HBaseTableCatalog.tableCatalog, user_orders_info_simple)
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .load().toDF()
    val s4=df.where(col("member_id")===member_id).drop(col("id"))
    s4.toJSON.collectAsList().toString
  }


  def job_brand_preference(): String =
  {
    def catalog =
      s"""{
         |"table":{"namespace":"default","name":"user_final"},
         |"rowkey":"id",
         |"columns":{
         |"id":{"cf":"rowkey","col":"id","type":"long"},
         |"job":{"cf":"cf","col":"job","type":"string"},
         |"brand_preference":{"cf":"cf", "col":"brand_preference", "type":"string"}
         |}
         |}""".stripMargin
    val df= spark.read
      .option(HBaseTableCatalog.tableCatalog, catalog)
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .load().toDF()
    val jobs=df.select(col("job").as("job"),col("brand_preference"))
      .where(col("brand_preference")=!="其他")
    val s4=jobs.groupBy("job","brand_preference")
      .agg(count("brand_preference")as "brand_preference_count").sort(col("brand_preference"),col("job"))
    s4.toJSON.collectAsList().toString
  }

  def job_payment_way(): String =
  {
    def catalog =
      s"""{
         |"table":{"namespace":"default","name":"user_final"},
         |"rowkey":"id",
         |"columns":{
         |"id":{"cf":"rowkey","col":"id","type":"long"},
         |"job":{"cf":"cf","col":"job","type":"string"},
         |"payment_way":{"cf":"cf", "col":"payment_way", "type":"string"}
         |}
         |}""".stripMargin
    val df= spark.read
      .option(HBaseTableCatalog.tableCatalog, catalog)
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .load().toDF()
    val jobs=df.select(col("job").as("job"),col("payment_way"))
    val s4=jobs.groupBy("job","payment_way")
      .agg(count("payment_way")as "payment_way_count").sort(col("payment_way"),col("job"))
    s4.toJSON.collectAsList().toString

  }

  def job_shopping_cycle(): String =
  {
    def catalog =
      s"""{
         |"table":{"namespace":"default","name":"user_final"},
         |"rowkey":"id",
         |"columns":{
         |"id":{"cf":"rowkey","col":"id","type":"long"},
         |"job":{"cf":"cf","col":"job","type":"string"},
         |"shopping_cycle":{"cf":"cf", "col":"shopping_cycle", "type":"string"}
         |}
         |}""".stripMargin
    val df= spark.read
      .option(HBaseTableCatalog.tableCatalog, catalog)
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .load().toDF()
    val jobs=df.select(col("job").as("job"),col("shopping_cycle"))
    val s4=jobs.groupBy("job","shopping_cycle")
      .agg(count("shopping_cycle")as "shopping_cycle_count").sort(col("shopping_cycle"),col("job"))
    s4.toJSON.collectAsList().toString
  }

  def job_ave_price_range(): String =
  {
    def catalog =
      s"""{
         |"table":{"namespace":"default","name":"user_final"},
         |"rowkey":"id",
         |"columns":{
         |"id":{"cf":"rowkey","col":"id","type":"long"},
         |"job":{"cf":"cf","col":"job","type":"string"},
         |"ave_price_range":{"cf":"cf", "col":"ave_price_range", "type":"string"}
         |}
         |}""".stripMargin
    val df= spark.read
      .option(HBaseTableCatalog.tableCatalog, catalog)
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .load().toDF()
    val jobs=df.select(col("job").as("job"),col("ave_price_range"))
    val s4=jobs.groupBy("job","ave_price_range")
      .agg(count("ave_price_range")as "ave_price_range_count").sort(col("ave_price_range"),col("job"))
    s4.toJSON.collectAsList().toString
  }

  def job_order_highest_range(): String =
  {
    def catalog =
      s"""{
         |"table":{"namespace":"default","name":"user_final"},
         |"rowkey":"id",
         |"columns":{
         |"id":{"cf":"rowkey","col":"id","type":"long"},
         |"job":{"cf":"cf","col":"job","type":"string"},
         |"order_highest_range":{"cf":"cf", "col":"order_highest_range", "type":"string"}
         |}
         |}""".stripMargin
    val df= spark.read
      .option(HBaseTableCatalog.tableCatalog, catalog)
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .load().toDF()
    val jobs=df.select(col("job").as("job"),col("order_highest_range"))
    val s4=jobs.groupBy("job","order_highest_range")
      .agg(count("order_highest_range")as "order_highest_range_count").sort(col("order_highest_range"),col("job"))
    s4.toJSON.collectAsList().toString
  }

  def job_frequency_range(): String =
  {
    def catalog =
      s"""{
         |"table":{"namespace":"default","name":"user_final"},
         |"rowkey":"id",
         |"columns":{
         |"id":{"cf":"rowkey","col":"id","type":"long"},
         |"job":{"cf":"cf","col":"job","type":"string"},
         |"frequency_range":{"cf":"cf", "col":"frequency_range(高,中,低)", "type":"string"}
         |}
         |}""".stripMargin
    val df= spark.read
      .option(HBaseTableCatalog.tableCatalog, catalog)
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .load().toDF()
    val jobs=df.select(col("job").as("job"),col("frequency_range"))
    val s4=jobs.groupBy("job","frequency_range")
      .agg(count("frequency_range")as "frequency_range_count").sort(col("frequency_range"),col("job"))
    s4.toJSON.collectAsList().toString
  }
  def job_exchange_item_rate(): String =
  {
    def catalog =
      s"""{
         |"table":{"namespace":"default","name":"user_final"},
         |"rowkey":"id",
         |"columns":{
         |"id":{"cf":"rowkey","col":"id","type":"long"},
         |"job":{"cf":"cf","col":"job","type":"string"},
         |"exchange_item_rate":{"cf":"cf", "col":"exchange_item_rate(高,中,低)", "type":"string"}
         |}
         |}""".stripMargin
    val df= spark.read
      .option(HBaseTableCatalog.tableCatalog, catalog)
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .load().toDF()
    val jobs=df.select(col("job").as("job"),col("exchange_item_rate"))
    val s4=jobs.groupBy("job","exchange_item_rate")
      .agg(count("exchange_item_rate")as "exchange_item_rate_count").sort(col("exchange_item_rate"),col("job"))
    s4.toJSON.collectAsList().toString
  }

  def job_return_item_rate(): String =
  {
    def catalog =
      s"""{
         |"table":{"namespace":"default","name":"user_final"},
         |"rowkey":"id",
         |"columns":{
         |"id":{"cf":"rowkey","col":"id","type":"long"},
         |"job":{"cf":"cf","col":"job","type":"string"},
         |"return_item_rate":{"cf":"cf", "col":"return_item_rate(高,中,低)", "type":"string"}
         |}
         |}""".stripMargin
    val df= spark.read
      .option(HBaseTableCatalog.tableCatalog, catalog)
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .load().toDF()
    val jobs=df.select(col("job").as("job"),col("return_item_rate"))
    val s4=jobs.groupBy("job","return_item_rate")
      .agg(count("return_item_rate")as "return_item_rate_count").sort(col("return_item_rate"),col("job"))
    s4.toJSON.collectAsList().toString
  }
  def birth_brand_preference(): String =
  {
    def catalog =
      s"""{
         |"table":{"namespace":"default","name":"user_final"},
         |"rowkey":"id",
         |"columns":{
         |"id":{"cf":"rowkey","col":"id","type":"long"},
         |"age_class":{"cf":"cf","col":"age_class","type":"string"},
         |"brand_preference":{"cf":"cf", "col":"brand_preference", "type":"string"}
         |}
         |}""".stripMargin
    val df= spark.read
      .option(HBaseTableCatalog.tableCatalog, catalog)
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .load().toDF()
    val births=df.select(col("age_class").as("birth"),col("brand_preference"))
      .where(col("brand_preference")=!="其他")
    val s4=births.groupBy("birth","brand_preference")
      .agg(count("brand_preference")as "brand_preference_count").sort(col("brand_preference"),col("birth"))
    s4.toJSON.collectAsList().toString
  }

  def birth_payment_way(): String =
  {
    def catalog =
      s"""{
         |"table":{"namespace":"default","name":"user_final"},
         |"rowkey":"id",
         |"columns":{
         |"id":{"cf":"rowkey","col":"id","type":"long"},
         |"age_class":{"cf":"cf","col":"age_class","type":"string"},
         |"payment_way":{"cf":"cf", "col":"payment_way", "type":"string"}
         |}
         |}""".stripMargin
    val df= spark.read
      .option(HBaseTableCatalog.tableCatalog, catalog)
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .load().toDF()
    val births=df.select(col("age_class").as("birth"),col("payment_way"))
    val s4=births.groupBy("birth","payment_way")
      .agg(count("payment_way")as "payment_way_count").sort(col("payment_way"),col("birth"))
    s4.toJSON.collectAsList().toString

  }

  def birth_shopping_cycle(): String =
  {
    def catalog =
      s"""{
         |"table":{"namespace":"default","name":"user_final"},
         |"rowkey":"id",
         |"columns":{
         |"id":{"cf":"rowkey","col":"id","type":"long"},
         |"age_class":{"cf":"cf","col":"age_class","type":"string"},
         |"shopping_cycle":{"cf":"cf", "col":"shopping_cycle", "type":"string"}
         |}
         |}""".stripMargin
    val df= spark.read
      .option(HBaseTableCatalog.tableCatalog, catalog)
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .load().toDF()
    val births=df.select(col("age_class").as("birth"),col("shopping_cycle"))
    val s4=births.groupBy("birth","shopping_cycle")
      .agg(count("shopping_cycle")as "shopping_cycle_count").sort(col("shopping_cycle"),col("birth"))
    s4.toJSON.collectAsList().toString
  }

  def birth_ave_price_range(): String =
  {
    def catalog =
      s"""{
         |"table":{"namespace":"default","name":"user_final"},
         |"rowkey":"id",
         |"columns":{
         |"id":{"cf":"rowkey","col":"id","type":"long"},
         |"age_class":{"cf":"cf","col":"age_class","type":"string"},
         |"ave_price_range":{"cf":"cf", "col":"ave_price_range", "type":"string"}
         |}
         |}""".stripMargin
    val df= spark.read
      .option(HBaseTableCatalog.tableCatalog, catalog)
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .load().toDF()
    val births=df.select(col("age_class").as("birth"),col("ave_price_range"))
    val s4=births.groupBy("birth","ave_price_range")
      .agg(count("ave_price_range")as "ave_price_range_count").sort(col("ave_price_range"),col("birth"))
    s4.toJSON.collectAsList().toString
  }

  def birth_order_highest_range(): String =
  {
    def catalog =
      s"""{
         |"table":{"namespace":"default","name":"user_final"},
         |"rowkey":"id",
         |"columns":{
         |"id":{"cf":"rowkey","col":"id","type":"long"},
         |"age_class":{"cf":"cf","col":"age_class","type":"string"},
         |"order_highest_range":{"cf":"cf", "col":"order_highest_range", "type":"string"}
         |}
         |}""".stripMargin
    val df= spark.read
      .option(HBaseTableCatalog.tableCatalog, catalog)
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .load().toDF()
    val births=df.select(col("age_class").as("birth"),col("order_highest_range"))
    val s4=births.groupBy("birth","order_highest_range")
      .agg(count("order_highest_range")as "order_highest_range_count").sort(col("order_highest_range"),col("birth"))
    s4.toJSON.collectAsList().toString
  }

  def birth_frequency_range(): String =
  {
    def catalog =
      s"""{
         |"table":{"namespace":"default","name":"user_final"},
         |"rowkey":"id",
         |"columns":{
         |"id":{"cf":"rowkey","col":"id","type":"long"},
         |"age_class":{"cf":"cf","col":"age_class","type":"string"},
         |"frequency_range":{"cf":"cf", "col":"frequency_range(高,中,低)", "type":"string"}
         |}
         |}""".stripMargin
    val df= spark.read
      .option(HBaseTableCatalog.tableCatalog, catalog)
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .load().toDF()
    val births=df.select(col("age_class").as("birth"),col("frequency_range"))
    val s4=births.groupBy("birth","frequency_range")
      .agg(count("frequency_range")as "frequency_range_count").sort(col("frequency_range"),col("birth"))
    s4.toJSON.collectAsList().toString
  }
  def birth_exchange_item_rate(): String =
  {
    def catalog =
      s"""{
         |"table":{"namespace":"default","name":"user_final"},
         |"rowkey":"id",
         |"columns":{
         |"id":{"cf":"rowkey","col":"id","type":"long"},
         |"age_class":{"cf":"cf","col":"age_class","type":"string"},
         |"exchange_item_rate":{"cf":"cf", "col":"exchange_item_rate(高,中,低)", "type":"string"}
         |}
         |}""".stripMargin
    val df= spark.read
      .option(HBaseTableCatalog.tableCatalog, catalog)
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .load().toDF()
    val births=df.select(col("age_class").as("birth"),col("exchange_item_rate"))
    val s4=births.groupBy("birth","exchange_item_rate")
      .agg(count("exchange_item_rate")as "exchange_item_rate_count").sort(col("exchange_item_rate"),col("birth"))
    s4.toJSON.collectAsList().toString
  }

  def birth_return_item_rate(): String =
  {
    def catalog =
      s"""{
         |"table":{"namespace":"default","name":"user_final"},
         |"rowkey":"id",
         |"columns":{
         |"id":{"cf":"rowkey","col":"id","type":"long"},
         |"age_class":{"cf":"cf","col":"age_class","type":"string"},
         |"return_item_rate":{"cf":"cf", "col":"return_item_rate(高,中,低)", "type":"string"}
         |}
         |}""".stripMargin
    val df= spark.read
      .option(HBaseTableCatalog.tableCatalog, catalog)
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .load().toDF()
    val births=df.select(col("age_class").as("birth"),col("return_item_rate"))
    val s4=births.groupBy("birth","return_item_rate")
      .agg(count("return_item_rate")as "return_item_rate_count").sort(col("return_item_rate"),col("birth"))
    s4.toJSON.collectAsList().toString
  }


  def getperson(a:String,b:String):String={
    def catalog =
      s"""{
         |"table":{"namespace":"default","name":"user_final"},
         |"rowkey":"id",
         |"columns":{
         |"id":{"cf":"rowkey","col":"id","type":"long"},
         |"e_mail":{"cf":"cf","col":"email","type":"string"},
         |"user_name":{"cf":"cf","col":"username","type":"string"},
         |"password":{"cf":"cf","col":"password","type":"string"},
         |"birthday":{"cf":"cf","col":"birthday","type":"string"},
         |"mobile":{"cf":"cf","col":"mobile","type":"string"},
         |"money":{"cf":"cf","col":"money","type":"string"},
         |"money_pwd":{"cf":"cf","col":"money_pwd","type":"string"},
         |"nick_name":{"cf":"cf","col":"nick_name","type":"string"},
         |"last_login_time":{"cf":"cf","col":"last_login_time","type":"string"},
         |"register_time":{"cf":"cf","col":"register_time","type":"string"},
         |"qq":{"cf":"cf","col":"qq","type":"string"},
         |"job":{"cf":"cf","col":"job","type":"string"},
         |"political_face":{"cf":"cf","col":"political_face","type":"string"},
         |"age_class":{"cf":"cf","col":"age_class","type":"string"},
         |"nationality":{"cf":"cf","col":"nationality","type":"string"},
         |"marriage":{"cf":"cf","col":"marriage","type":"string"},
         |"is_in_blacklist":{"cf":"cf","col":"is_in_blacklist","type":"string"},
         |"constellation":{"cf":"cf","col":"constellation","type":"string"},
         |"scanned_goods":{"cf":"cf", "col":"scanned_goods", "type":"string"},
         |"product_type":{"cf":"cf", "col":"product_type", "type":"string"},
         |"good_bought":{"cf":"cf", "col":"good_bought", "type":"string"},
         |"brand_preference":{"cf":"cf", "col":"brand_preference", "type":"string"},
         |"product_name":{"cf":"cf", "col":"productName", "type":"string"},
         |"payment_way":{"cf":"cf", "col":"payment_way", "type":"string"},
         |"shopping_cycle":{"cf":"cf", "col":"shopping_cycle", "type":"string"},
         |"ave_price":{"cf":"cf", "col":"ave_price", "type":"string"},
         |"ave_price_range":{"cf":"cf", "col":"ave_price_range", "type":"string"},
         |"order_count":{"cf":"cf", "col":"order_count","type":"long"},
         |"order_highest":{"cf":"cf", "col":"order_highest", "type":"string"},
         |"order_highest_range":{"cf":"cf", "col":"order_highest_range", "type":"string"},
         |"frequency":{"cf":"cf", "col":"frequency", "type":"string"},
         |"frequency_range(高,中,低)":{"cf":"cf", "col":"frequency_range(高,中,低)", "type":"string"},
         |"exchange_item_rate":{"cf":"cf", "col":"exchange_item_rate", "type":"string"},
         |"return_item_rate":{"cf":"cf", "col":"return_item_rate", "type":"string"},
         |"exchange_item_rate(高,中,低)":{"cf":"cf", "col":"exchange_item_rate(高,中,低)", "type":"string"},
         |"return_item_rate(高,中,低)":{"cf":"cf", "col":"return_item_rate(高,中,低)", "type":"string"},
         |"consumption":{"cf":"cf", "col":"consumption", "type":"string"},
         |"scanned_page":{"cf":"cf", "col":"scanned_page", "type":"string"},
         |"scan_total_time":{"cf":"cf", "col":"scan_total_time", "type":"string"},
         |"count_scan":{"cf":"cf", "col":"count_scan", "type":"string"},
         |"log_frequency":{"cf":"cf", "col":"log_frequency", "type":"string"},
         |"log_time":{"cf":"cf", "col":"log_time", "type":"string"},
         |"count_log":{"cf":"cf", "col":"count_log", "type":"string"},
         |"log_time_arrange":{"cf":"cf", "col":"log_time_arrange", "type":"string"}
         |}
         |}""".stripMargin
    val df= spark.read
      .option(HBaseTableCatalog.tableCatalog, catalog)
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .load().toDF()

    val df2=df.where(col("user_name") === a and (col("password") === b)

    )
//    df2.show()
    if(df2.isEmpty) {
      return "error"
    }else{
      df2.toJSON.collectAsList().toString
    }
  }

  def user_discount(id:String):String={
    def user_discount=
      s"""{
         |  "table":{"namespace":"default", "name":"user_discount"},
         |  "rowkey":"Id",
         |  "columns":{
         |    "Id":{"cf":"rowkey", "col":"Id", "type":"long"},
         |    "predict":{"cf":"cf", "col":"predict", "type":"long"}
         |  }
         |}""".stripMargin
    var df= spark.read
      .option(HBaseTableCatalog.tableCatalog, user_discount)
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .load().toDF()
    df=df.where(col("id")===id)
    df.toJSON.collectAsList().toString
  }

  def user_class_1(username:String):String={
    def user_discount=
      s"""{
         |  "table":{"namespace":"default", "name":"user_class_1"},
         |  "rowkey":"Id",
         |  "columns":{
         |    "Id":{"cf":"rowkey", "col":"Id", "type":"long"},
         |    "username":{"cf":"rowkey", "col":"username", "type":"string"},
         |    "predict":{"cf":"cf", "col":"predict", "type":"string"}
         |  }
         |}""".stripMargin
    var df= spark.read
      .option(HBaseTableCatalog.tableCatalog, user_discount)
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .load().toDF()
    df=df.where(col("username")===username)
    df.toJSON.collectAsList().toString
  }


  def search(a:String):String={
    def catalog =
      s"""{
         |"table":{"namespace":"default","name":"user_final"},
         |"rowkey":"id",
         |"columns":{
         |"id":{"cf":"rowkey","col":"id","type":"long"},
         |"e_mail":{"cf":"cf","col":"email","type":"string"},
         |"user_name":{"cf":"cf","col":"username","type":"string"},
         |"password":{"cf":"cf","col":"password","type":"string"},
         |"birthday":{"cf":"cf","col":"birthday","type":"string"},
         |"mobile":{"cf":"cf","col":"mobile","type":"string"},
         |"money":{"cf":"cf","col":"money","type":"string"},
         |"money_pwd":{"cf":"cf","col":"money_pwd","type":"string"},
         |"nick_name":{"cf":"cf","col":"nick_name","type":"string"},
         |"last_login_time":{"cf":"cf","col":"last_login_time","type":"string"},
         |"register_time":{"cf":"cf","col":"register_time","type":"string"},
         |"qq":{"cf":"cf","col":"qq","type":"string"},
         |"job":{"cf":"cf","col":"job","type":"string"},
         |"political_face":{"cf":"cf","col":"political_face","type":"string"},
         |"age_class":{"cf":"cf","col":"age_class","type":"string"},
         |"nationality":{"cf":"cf","col":"nationality","type":"string"},
         |"marriage":{"cf":"cf","col":"marriage","type":"string"},
         |"is_in_blacklist":{"cf":"cf","col":"is_in_blacklist","type":"string"},
         |"constellation":{"cf":"cf","col":"constellation","type":"string"},
         |"scanned_goods":{"cf":"cf", "col":"scanned_goods", "type":"string"},
         |"product_type":{"cf":"cf", "col":"product_type", "type":"string"},
         |"good_bought":{"cf":"cf", "col":"good_bought", "type":"string"},
         |"brand_preference":{"cf":"cf", "col":"brand_preference", "type":"string"},
         |"product_name":{"cf":"cf", "col":"productName", "type":"string"},
         |"payment_way":{"cf":"cf", "col":"payment_way", "type":"string"},
         |"shopping_cycle":{"cf":"cf", "col":"shopping_cycle", "type":"string"},
         |"ave_price":{"cf":"cf", "col":"ave_price", "type":"string"},
         |"ave_price_range":{"cf":"cf", "col":"ave_price_range", "type":"string"},
         |"order_count":{"cf":"cf", "col":"order_count","type":"long"},
         |"order_highest":{"cf":"cf", "col":"order_highest", "type":"string"},
         |"order_highest_range":{"cf":"cf", "col":"order_highest_range", "type":"string"},
         |"frequency":{"cf":"cf", "col":"frequency", "type":"string"},
         |"frequency_range(高,中,低)":{"cf":"cf", "col":"frequency_range(高,中,低)", "type":"string"},
         |"exchange_item_rate":{"cf":"cf", "col":"exchange_item_rate", "type":"string"},
         |"return_item_rate":{"cf":"cf", "col":"return_item_rate", "type":"string"},
         |"exchange_item_rate(高,中,低)":{"cf":"cf", "col":"exchange_item_rate(高,中,低)", "type":"string"},
         |"return_item_rate(高,中,低)":{"cf":"cf", "col":"return_item_rate(高,中,低)", "type":"string"},
         |"consumption":{"cf":"cf", "col":"consumption", "type":"string"},
         |"scanned_page":{"cf":"cf", "col":"scanned_page", "type":"string"},
         |"scan_total_time":{"cf":"cf", "col":"scan_total_time", "type":"string"},
         |"count_scan":{"cf":"cf", "col":"count_scan", "type":"string"},
         |"log_frequency":{"cf":"cf", "col":"log_frequency", "type":"string"},
         |"log_time":{"cf":"cf", "col":"log_time", "type":"string"},
         |"count_log":{"cf":"cf", "col":"count_log", "type":"string"},
         |"log_time_arrange":{"cf":"cf", "col":"log_time_arrange", "type":"string"}
         |}
         |}""".stripMargin
    val df= spark.read
      .option(HBaseTableCatalog.tableCatalog, catalog)
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .load().toDF()

    val df2=df.where(col("mobile") === a)
    //    df2.show()
    if(df2.isEmpty) {
      return "error"
    }else{
      df2.toJSON.collectAsList().toString
    }
  }


  def getgender():String={
    def catalog =
      s"""{
         |"table":{"namespace":"default", "name":"gender_sum"},
         |"rowkey":"gender",
         |"columns":{
         |"gender":{"cf":"rowkey", "col":"gender", "type":"string"},
         |"count":{"cf":"cf", "col":"count", "type":"long"}
         |}
         |}""".stripMargin

    val df= spark.read
      .option(HBaseTableCatalog.tableCatalog, catalog)
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .load().toDF()
    val df2Array: Array[(String, Any)] = df.collect().map { row => (row(0).toString, row(1)) }
    println(df.collect())
    println(df2Array)
    val jsonData: Array[JSONObject] = df2Array.map { i =>
      new JSONObject(Map(i._1 -> i._2))
    }
    val jsTest = jsonData.mkString(",").replace("},{",",")
   // df.toJSON.collectAsList().toString
    jsTest
  }

  def getjob():String={
    def catalog =
      s"""{
         |"table":{"namespace":"default", "name":"cal_job"},
         |"rowkey":"job",
         |"columns":{
         |"job":{"cf":"rowkey", "col":"job", "type":"string"},
         |"count":{"cf":"cf", "col":"count", "type":"long"}
         |}
         |}""".stripMargin
    val df= spark.read
      .option(HBaseTableCatalog.tableCatalog, catalog)
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .load().toDF()
    //val jsonStr:String = df.toJSON.collectAsList.toString
    //return jsonStr
    val df2Array: Array[(String, Any)] = df.collect().map { row => (row(0).toString, row(1)) }
    val jsonData: Array[JSONObject] = df2Array.map { i =>
      new JSONObject(Map(i._1 -> i._2))
    }
    val jsTest = jsonData.mkString(",").replace("},{",",")
    // df.toJSON.collectAsList().toString
    jsTest
  }

  def getbirth():String={
    def catalog =
      s"""{
         |"table":{"namespace":"default", "name":"cal_birth"},
         |"rowkey":"birth",
         |"columns":{
         |"birth":{"cf":"rowkey", "col":"birth", "type":"string"},
         |"count":{"cf":"cf", "col":"count", "type":"long"}
         |}
         |}""".stripMargin
    val df= spark.read
      .option(HBaseTableCatalog.tableCatalog, catalog)
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .load().toDF()
    val jsonStr:String = df.toJSON.collectAsList.toString
    val df2Array: Array[(String, Any)] = df.collect().map { row => (row(0).toString, row(1)) }
    val jsonData: Array[JSONObject] = df2Array.map { i =>
      new JSONObject(Map(i._1 -> i._2))
    }
    val jsTest = jsonData.mkString(",").replace("},{",",")
    // df.toJSON.collectAsList().toString
    jsTest
  }

  def getnationality():String={
    def catalog =
      s"""{
         |"table":{"namespace":"default", "name":"cal_nationality"},
         |"rowkey":"nationality",
         |"columns":{
         |"nationality":{"cf":"rowkey", "col":"nationality", "type":"string"},
         |"count":{"cf":"cf", "col":"count", "type":"long"}
         |}
         |}""".stripMargin
    val df= spark.read
      .option(HBaseTableCatalog.tableCatalog, catalog)
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .load().toDF()
    val jsonStr:String = df.toJSON.collectAsList.toString
    val df2Array: Array[(String, Any)] = df.collect().map { row => (row(0).toString, row(1)) }
    val jsonData: Array[JSONObject] = df2Array.map { i =>
      new JSONObject(Map(i._1 -> i._2))
    }
    val jsTest = jsonData.mkString(",").replace("},{",",")
    // df.toJSON.collectAsList().toString
    jsTest
  }

  def getpolitical_face():String={
    def catalog =
      s"""{
         |"table":{"namespace":"default", "name":"cal_political_face"},
         |"rowkey":"political_face",
         |"columns":{
         |"political_face":{"cf":"rowkey", "col":"political_face", "type":"string"},
         |"count":{"cf":"cf", "col":"count", "type":"long"}
         |}
         |}""".stripMargin
    val df= spark.read
      .option(HBaseTableCatalog.tableCatalog, catalog)
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .load().toDF()
    val jsonStr:String = df.toJSON.collectAsList.toString
    val df2Array: Array[(String, Any)] = df.collect().map { row => (row(0).toString, row(1)) }
    val jsonData: Array[JSONObject] = df2Array.map { i =>
      new JSONObject(Map(i._1 -> i._2))
    }
    val jsTest = jsonData.mkString(",").replace("},{",",")
    // df.toJSON.collectAsList().toString
    jsTest
  }

  def getbrand_preference():String={
    def catalog =
      s"""{
         |"table":{"namespace":"default", "name":"cal_brand_preference"},
         |"rowkey":"brand_preference",
         |"columns":{
         |"brand_preference":{"cf":"rowkey", "col":"brand_preference", "type":"string"},
         |"count":{"cf":"cf", "col":"count", "type":"long"}
         |}
         |}""".stripMargin
    val df= spark.read
      .option(HBaseTableCatalog.tableCatalog, catalog)
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .load().toDF()
    val jsonStr:String = df.toJSON.collectAsList.toString
    val df2Array: Array[(String, Any)] = df.collect().map { row => (row(0).toString, row(1)) }
    val jsonData: Array[JSONObject] = df2Array.map { i =>
      new JSONObject(Map(i._1 -> i._2))
    }
    val jsTest = jsonData.mkString(",").replace("},{",",")
    // df.toJSON.collectAsList().toString
    jsTest
  }

  def getmarriage():String={
    def catalog =
      s"""{
         |"table":{"namespace":"default", "name":"cal_marriage"},
         |"rowkey":"marriage",
         |"columns":{
         |"marriage":{"cf":"rowkey", "col":"marriage", "type":"string"},
         |"count":{"cf":"cf", "col":"count", "type":"long"}
         |}
         |}""".stripMargin
    val df= spark.read
      .option(HBaseTableCatalog.tableCatalog, catalog)
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .load().toDF()
    val jsonStr:String = df.toJSON.collectAsList.toString
    val df2Array: Array[(String, Any)] = df.collect().map { row => (row(0).toString, row(1)) }
    val jsonData: Array[JSONObject] = df2Array.map { i =>
      new JSONObject(Map(i._1 -> i._2))
    }
    val jsTest = jsonData.mkString(",").replace("},{",",")
    // df.toJSON.collectAsList().toString
    jsTest
  }

  def getconsumption_ablity():String={
    def catalog =
      s"""{
         |"table":{"namespace":"default", "name":"cal_consumption_ablity"},
         |"rowkey":"consumption_ablity",
         |"columns":{
         |"consumption_ablity":{"cf":"rowkey", "col":"consumption_ablity", "type":"string"},
         |"count":{"cf":"cf", "col":"count", "type":"long"}
         |}
         |}""".stripMargin
    val df= spark.read
      .option(HBaseTableCatalog.tableCatalog, catalog)
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .load().toDF()
    val jsonStr:String = df.toJSON.collectAsList.toString
    val df2Array: Array[(String, Any)] = df.collect().map { row => (row(0).toString, row(1)) }
    val jsonData: Array[JSONObject] = df2Array.map { i =>
      new JSONObject(Map(i._1 -> i._2))
    }
    val jsTest = jsonData.mkString(",").replace("},{",",")
    // df.toJSON.collectAsList().toString
    jsTest
  }

  def gettest():String={
    def catalog =
      s"""{
         |"table":{"namespace":"default", "name":"birth_colony_shopping_cycle"},
         |"rowkey":"birth",
         |"columns":{
         |"birth":{"cf":"rowkey", "col":"birth", "type":"string"},
         |"shopping_cycle":{"cf":"cf", "col":"shopping_cycle", "type":"string"}
         |}
         |}""".stripMargin
    val df= spark.read
      .option(HBaseTableCatalog.tableCatalog, catalog)
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .load().toDF()
     df.toJSON.collectAsList().toString
  }

  def getshopping_cycle():String={
    def catalog =
      s"""{
         |"table":{"namespace":"default", "name":"cal_shopping_cycle"},
         |"rowkey":"shopping_cycle",
         |"columns":{
         |"shopping_cycle":{"cf":"rowkey", "col":"shopping_cycle", "type":"string"},
         |"count":{"cf":"cf", "col":"count", "type":"long"}
         |}
         |}""".stripMargin
    val df= spark.read
      .option(HBaseTableCatalog.tableCatalog, catalog)
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .load().toDF()
    val jsonStr:String = df.toJSON.collectAsList.toString
    val df2Array: Array[(String, Any)] = df.collect().map { row => (row(0).toString, row(1)) }
    val jsonData: Array[JSONObject] = df2Array.map { i =>
      new JSONObject(Map(i._1 -> i._2))
    }
    val jsTest = jsonData.mkString(",").replace("},{",",")
    // df.toJSON.collectAsList().toString
    jsTest
  }

  def getave_price_range():String={
    def catalog =
      s"""{
         |"table":{"namespace":"default", "name":"cal_ave_price_range"},
         |"rowkey":"ave_price_range",
         |"columns":{
         |"ave_price_range":{"cf":"rowkey", "col":"ave_price_range", "type":"string"},
         |"count":{"cf":"cf", "col":"count", "type":"long"}
         |}
         |}""".stripMargin
    val df= spark.read
      .option(HBaseTableCatalog.tableCatalog, catalog)
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .load().toDF()
    val jsonStr:String = df.toJSON.collectAsList.toString
    val df2Array: Array[(String, Any)] = df.collect().map { row => (row(0).toString, row(1)) }
    val jsonData: Array[JSONObject] = df2Array.map { i =>
      new JSONObject(Map(i._1 -> i._2))
    }
    val jsTest = jsonData.mkString(",").replace("},{",",")
    // df.toJSON.collectAsList().toString
    jsTest
  }

  def getorder_highest_range():String={
    def catalog =
      s"""{
         |"table":{"namespace":"default", "name":"cal_order_highest_range"},
         |"rowkey":"order_highest_range",
         |"columns":{
         |"order_highest_range":{"cf":"rowkey", "col":"order_highest_range", "type":"string"},
         |"count":{"cf":"cf", "col":"count", "type":"long"}
         |}
         |}""".stripMargin
    val df= spark.read
      .option(HBaseTableCatalog.tableCatalog, catalog)
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .load().toDF()
    val jsonStr:String = df.toJSON.collectAsList.toString
    val df2Array: Array[(String, Any)] = df.collect().map { row => (row(0).toString, row(1)) }
    val jsonData: Array[JSONObject] = df2Array.map { i =>
      new JSONObject(Map(i._1 -> i._2))
    }
    val jsTest = jsonData.mkString(",").replace("},{",",")
    // df.toJSON.collectAsList().toString
    jsTest
  }

  def getlog_frequency():String={
    def catalog =
      s"""{
         |"table":{"namespace":"default", "name":"cal_log_frequency"},
         |"rowkey":"log_frequency",
         |"columns":{
         |"log_frequency":{"cf":"rowkey", "col":"log_frequency", "type":"string"},
         |"count":{"cf":"cf", "col":"count", "type":"long"}
         |}
         |}""".stripMargin
    val df= spark.read
      .option(HBaseTableCatalog.tableCatalog, catalog)
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .load().toDF()
    val jsonStr:String = df.toJSON.collectAsList.toString
    val df2Array: Array[(String, Any)] = df.collect().map { row => (row(0).toString, row(1)) }
    val jsonData: Array[JSONObject] = df2Array.map { i =>
      new JSONObject(Map(i._1 -> i._2))
    }
    val jsTest = jsonData.mkString(",").replace("},{",",")
    // df.toJSON.collectAsList().toString
    jsTest
  }


}