package gala2

import gala2.read.spark
import org.apache.spark.sql.catalyst.dsl.expressions.{DslExpression, DslSymbol, StringToAttributeConversionHelper}
import org.apache.spark.sql.execution.datasources.hbase.HBaseTableCatalog
import org.apache.spark.sql.functions._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.execution.datasources.hbase.HBaseTableCatalog
import org.apache.spark.sql.functions.{col, to_timestamp, when, year}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

import scala.util.parsing.json.{JSON, JSONArray, JSONObject}

object colony_read {

  def main(args: Array[String]):Unit= {
    //birth_colony_brand_preference()
   // birth_colony_shopping_cycle()
   // birth_colony_ave_price_range()
   // birth_colony_order_highest_range()
  //  birth_colony_frequency_range()
  //  birth_colony_exchange_item_rate()
  //  birth_colony_return_item_rate()
   def catalog_brand_preference =
     s"""{
        |"table":{"namespace":"default", "name":"birth_colony_brand_preference"},
        |"rowkey":"birth",
        |"columns":{
        |"birth":{"cf":"rowkey", "col":"birth", "type":"string"},
        |"brand_preference":{"cf":"cf", "col":"brand_preference", "type":"string"}
        |}
        |}""".stripMargin
    def catalog_shopping_cycle =
      s"""{
         |"table":{"namespace":"default", "name":"birth_colony_shopping_cycle"},
         |"rowkey":"birth",
         |"columns":{
         |"birth":{"cf":"rowkey", "col":"birth", "type":"string"},
         |"shopping_cycle":{"cf":"cf", "col":"shopping_cycle", "type":"string"}
         |}
         |}""".stripMargin
    def catalog_ave_price_range =
      s"""{
         |"table":{"namespace":"default", "name":"birth_colony_ave_price_range"},
         |"rowkey":"birth",
         |"columns":{
         |"birth":{"cf":"rowkey", "col":"birth", "type":"string"},
         |"ave_price_range":{"cf":"cf", "col":"ave_price_range", "type":"string"}
         |}
         |}""".stripMargin
    def catalog_order_highest_range =
      s"""{
         |"table":{"namespace":"default", "name":"birth_colony_order_highest_range"},
         |"rowkey":"birth",
         |"columns":{
         |"birth":{"cf":"rowkey", "col":"birth", "type":"string"},
         |"order_highest_range":{"cf":"cf", "col":"order_highest_range", "type":"string"}
         |}
         |}""".stripMargin
    def catalog_colony_frequency_range =
      s"""{
         |"table":{"namespace":"default", "name":"birth_colony_frequency_range"},
         |"rowkey":"birth",
         |"columns":{
         |"birth":{"cf":"rowkey", "col":"birth", "type":"string"},
         |"frequency_range":{"cf":"cf", "col":"frequency_range", "type":"string"}
         |}
         |}""".stripMargin
    def catalog_colony_exchange_item_rate =
      s"""{
         |"table":{"namespace":"default", "name":"birth_colony_exchange_item_rate"},
         |"rowkey":"birth",
         |"columns":{
         |"birth":{"cf":"rowkey", "col":"birth", "type":"string"},
         |"exchange_item_rate":{"cf":"cf", "col":"exchange_item_rate", "type":"string"}
         |}
         |}""".stripMargin
    def catalog_colony_return_item_rate =
      s"""{
         |"table":{"namespace":"default", "name":"birth_colony_return_item_rate"},
         |"rowkey":"birth",
         |"columns":{
         |"birth":{"cf":"rowkey", "col":"birth", "type":"string"},
         |"return_item_rate":{"cf":"cf", "col":"return_item_rate", "type":"string"}
         |}
         |}""".stripMargin
    val spark = SparkSession.builder()
      .appName("shc test")
      .master("local[10]")
      .getOrCreate()
    import spark.implicits._
    val df_brand_preference= spark.read
      .option(HBaseTableCatalog.tableCatalog, catalog_brand_preference)
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .load().toDF()
    val df_ave_price_range= spark.read
      .option(HBaseTableCatalog.tableCatalog, catalog_ave_price_range)
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .load().toDF()
    val df_shopping_cycle= spark.read
      .option(HBaseTableCatalog.tableCatalog, catalog_shopping_cycle)
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .load().toDF()
    val df_order_highest_range = spark.read
      .option(HBaseTableCatalog.tableCatalog, catalog_order_highest_range )
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .load().toDF()
    val df_colony_frequency_range = spark.read
      .option(HBaseTableCatalog.tableCatalog, catalog_colony_frequency_range )
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .load().toDF()
    val df_colony_exchange_item_rate = spark.read
      .option(HBaseTableCatalog.tableCatalog, catalog_colony_exchange_item_rate )
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .load().toDF()
    val df_colony_return_item_rate = spark.read
      .option(HBaseTableCatalog.tableCatalog, catalog_colony_return_item_rate )
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .load().toDF()
    val fi1=df_brand_preference.join(df_ave_price_range,df_brand_preference.col("birth")===df_ave_price_range.col("birth")).drop(df_ave_price_range.col("birth"))
    val fi2=fi1.join(df_shopping_cycle,fi1.col("birth")===df_shopping_cycle.col("birth")).drop(df_shopping_cycle.col("birth"))
    val fi3=fi2.join(df_order_highest_range,fi1.col("birth")===df_order_highest_range.col("birth")).drop(df_order_highest_range.col("birth"))
    val fi4=fi3.join(df_colony_frequency_range,fi1.col("birth")===df_colony_frequency_range.col("birth")).drop(df_colony_frequency_range.col("birth"))
    val fi5=fi4.join(df_colony_exchange_item_rate,fi1.col("birth")===df_colony_exchange_item_rate.col("birth")).drop(df_colony_exchange_item_rate.col("birth"))
    val fi6=fi5.join(df_colony_return_item_rate,fi1.col("birth")===df_colony_return_item_rate.col("birth")).drop(df_colony_return_item_rate.col("birth"))

    fi6.show(false)
  }

  def birth_colony_brand_preference():Unit= {
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

         |"brand_preference":{"cf":"cf", "col":"brand_preference", "type":"string"},
         |"payment_way":{"cf":"cf", "col":"payment_way", "type":"string"},
         |"ave_price_range":{"cf":"cf", "col":"ave_price_range", "type":"string"},
         |"order_highest_range":{"cf":"cf", "col":"order_highest_range", "type":"string"},
         |"frequency_range(高,中,低)":{"cf":"cf", "col":"frequency_range(高,中,低)", "type":"string"},
         |"exchange_item_rate(高,中,低)":{"cf":"cf", "col":"exchange_item_rate(高,中,低)", "type":"string"},
         |"return_item_rate(高,中,低)":{"cf":"cf", "col":"return_item_rate(高,中,低)", "type":"string"}

         |}
         |}""".stripMargin
    val spark = SparkSession.builder()
      .appName("shc test")
      .master("local[10]")
      .getOrCreate()
    import spark.implicits._
    val df= spark.read
      .option(HBaseTableCatalog.tableCatalog, catalog)
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .load().toDF()
    val births=df.select(col("age_class").as("birth"),col("brand_preference"))
      .where(col("brand_preference")=!="其他")
    val s4=births.groupBy("birth","brand_preference")
      .agg(count("brand_preference")as "brand_preference_count")
      .sort(col("brand_preference_count").desc).dropDuplicates("birth")
    s4.show(false)
    val s5=s4.select(col("birth"),col("brand_preference"))
    def catalogWrite =
      s"""{
         |"table":{"namespace":"default", "name":"birth_colony_brand_preference"},
         |"rowkey":"birth",
         |"columns":{
         |"birth":{"cf":"rowkey", "col":"birth", "type":"string"},
         |"brand_preference":{"cf":"cf", "col":"brand_preference", "type":"string"}
         |}
         |}""".stripMargin
    s5.write
      .option(HBaseTableCatalog.tableCatalog, catalogWrite)
      .option(HBaseTableCatalog.newTable, "5")
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .save()
  }

  def birth_colony_shopping_cycle():Unit= {
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

         |"brand_preference":{"cf":"cf", "col":"brand_preference", "type":"string"},
         |"payment_way":{"cf":"cf", "col":"payment_way", "type":"string"},
         |"shopping_cycle":{"cf":"cf", "col":"shopping_cycle", "type":"string"},
         |"ave_price_range":{"cf":"cf", "col":"ave_price_range", "type":"string"},
         |"order_highest_range":{"cf":"cf", "col":"order_highest_range", "type":"string"},
         |"frequency_range(高,中,低)":{"cf":"cf", "col":"frequency_range(高,中,低)", "type":"string"},
         |"exchange_item_rate(高,中,低)":{"cf":"cf", "col":"exchange_item_rate(高,中,低)", "type":"string"},
         |"return_item_rate(高,中,低)":{"cf":"cf", "col":"return_item_rate(高,中,低)", "type":"string"}

         |}
         |}""".stripMargin
    val spark = SparkSession.builder()
      .appName("shc test")
      .master("local[10]")
      .getOrCreate()
    import spark.implicits._
    val df= spark.read
      .option(HBaseTableCatalog.tableCatalog, catalog)
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .load().toDF()
    val births=df.select(col("age_class").as("birth"),col("shopping_cycle"))
    val s4=births.groupBy("birth","shopping_cycle")
      .agg(count("shopping_cycle")as "shopping_cycle_count")
      .sort(col("shopping_cycle_count").desc).dropDuplicates("birth")
    s4.show(false)
    val s5=s4.select(col("birth"),col("shopping_cycle"))
    def catalogWrite =
      s"""{
         |"table":{"namespace":"default", "name":"birth_colony_shopping_cycle"},
         |"rowkey":"birth",
         |"columns":{
         |"birth":{"cf":"rowkey", "col":"birth", "type":"string"},
         |"shopping_cycle":{"cf":"cf", "col":"shopping_cycle", "type":"string"}
         |}
         |}""".stripMargin
    s5.write
      .option(HBaseTableCatalog.tableCatalog, catalogWrite)
      .option(HBaseTableCatalog.newTable, "5")
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .save()
  }

  def birth_colony_ave_price_range():Unit= {
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

         |"brand_preference":{"cf":"cf", "col":"brand_preference", "type":"string"},
         |"payment_way":{"cf":"cf", "col":"payment_way", "type":"string"},
         |"ave_price_range":{"cf":"cf", "col":"ave_price_range", "type":"string"},
         |"order_highest_range":{"cf":"cf", "col":"order_highest_range", "type":"string"},
         |"frequency_range(高,中,低)":{"cf":"cf", "col":"frequency_range(高,中,低)", "type":"string"},
         |"exchange_item_rate(高,中,低)":{"cf":"cf", "col":"exchange_item_rate(高,中,低)", "type":"string"},
         |"return_item_rate(高,中,低)":{"cf":"cf", "col":"return_item_rate(高,中,低)", "type":"string"}

         |}
         |}""".stripMargin
    val spark = SparkSession.builder()
      .appName("shc test")
      .master("local[10]")
      .getOrCreate()
    import spark.implicits._
    val df= spark.read
      .option(HBaseTableCatalog.tableCatalog, catalog)
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .load().toDF()
    val births=df.select(col("age_class").as("birth"),col("ave_price_range"))
    val s4=births.groupBy("birth","ave_price_range")
      .agg(count("ave_price_range")as "ave_price_range_count")
      .sort(col("ave_price_range_count").desc).dropDuplicates("birth")
    val s5=s4.select(col("birth"),col("ave_price_range"))
    s4.show(false)
    def catalogWrite =
      s"""{
         |"table":{"namespace":"default", "name":"birth_colony_ave_price_range"},
         |"rowkey":"birth",
         |"columns":{
         |"birth":{"cf":"rowkey", "col":"birth", "type":"string"},
         |"ave_price_range":{"cf":"cf", "col":"ave_price_range", "type":"string"}
         |}
         |}""".stripMargin
    s5.write
      .option(HBaseTableCatalog.tableCatalog, catalogWrite)
      .option(HBaseTableCatalog.newTable, "5")
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .save()
  }

  def birth_colony_order_highest_range():Unit= {
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

         |"brand_preference":{"cf":"cf", "col":"brand_preference", "type":"string"},
         |"payment_way":{"cf":"cf", "col":"payment_way", "type":"string"},
         |"order_highest_range":{"cf":"cf", "col":"order_highest_range", "type":"string"},
         |"frequency_range(高,中,低)":{"cf":"cf", "col":"frequency_range(高,中,低)", "type":"string"},
         |"exchange_item_rate(高,中,低)":{"cf":"cf", "col":"exchange_item_rate(高,中,低)", "type":"string"},
         |"return_item_rate(高,中,低)":{"cf":"cf", "col":"return_item_rate(高,中,低)", "type":"string"}

         |}
         |}""".stripMargin
    val spark = SparkSession.builder()
      .appName("shc test")
      .master("local[10]")
      .getOrCreate()
    import spark.implicits._
    val df= spark.read
      .option(HBaseTableCatalog.tableCatalog, catalog)
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .load().toDF()
    val births=df.select(col("age_class").as("birth"),col("order_highest_range"))
    val s4=births.groupBy("birth","order_highest_range")
      .agg(count("order_highest_range")as "order_highest_range_count")
      .sort(col("order_highest_range_count").desc).dropDuplicates("birth")
    val s5=s4.select(col("birth"),col("order_highest_range"))
    s4.show(false)
    def catalogWrite =
      s"""{
         |"table":{"namespace":"default", "name":"birth_colony_order_highest_range"},
         |"rowkey":"birth",
         |"columns":{
         |"birth":{"cf":"rowkey", "col":"birth", "type":"string"},
         |"order_highest_range":{"cf":"cf", "col":"order_highest_range", "type":"string"}
         |}
         |}""".stripMargin
    s5.write
      .option(HBaseTableCatalog.tableCatalog, catalogWrite)
      .option(HBaseTableCatalog.newTable, "5")
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .save()
  }

  def birth_colony_frequency_range():Unit= {
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

         |"brand_preference":{"cf":"cf", "col":"brand_preference", "type":"string"},
         |"payment_way":{"cf":"cf", "col":"payment_way", "type":"string"},
         |"frequency_range(高,中,低)":{"cf":"cf", "col":"frequency_range(高,中,低)", "type":"string"},
         |"exchange_item_rate(高,中,低)":{"cf":"cf", "col":"exchange_item_rate(高,中,低)", "type":"string"},
         |"return_item_rate(高,中,低)":{"cf":"cf", "col":"return_item_rate(高,中,低)", "type":"string"}

         |}
         |}""".stripMargin
    val spark = SparkSession.builder()
      .appName("shc test")
      .master("local[10]")
      .getOrCreate()
    import spark.implicits._
    val df= spark.read
      .option(HBaseTableCatalog.tableCatalog, catalog)
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .load().toDF()
    val births=df.select(col("age_class").as("birth"),col("frequency_range(高,中,低)"))
    val s4=births.groupBy("birth","frequency_range(高,中,低)")
      .agg(count("frequency_range(高,中,低)")as "frequency_range(高,中,低)_count")
      .sort(col("frequency_range(高,中,低)_count").desc).dropDuplicates("birth")
    val s5=s4.select(col("birth"),col("frequency_range(高,中,低)").as("frequency_range"))
    def catalogWrite =
      s"""{
         |"table":{"namespace":"default", "name":"birth_colony_frequency_range"},
         |"rowkey":"birth",
         |"columns":{
         |"birth":{"cf":"rowkey", "col":"birth", "type":"string"},
         |"frequency_range":{"cf":"cf", "col":"frequency_range", "type":"string"}
         |}
         |}""".stripMargin
    s5.write
      .option(HBaseTableCatalog.tableCatalog, catalogWrite)
      .option(HBaseTableCatalog.newTable, "5")
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .save()
  }

  def birth_colony_exchange_item_rate():Unit= {
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

         |"brand_preference":{"cf":"cf", "col":"brand_preference", "type":"string"},
         |"payment_way":{"cf":"cf", "col":"payment_way", "type":"string"},
         |"exchange_item_rate(高,中,低)":{"cf":"cf", "col":"exchange_item_rate(高,中,低)", "type":"string"},
         |"return_item_rate(高,中,低)":{"cf":"cf", "col":"return_item_rate(高,中,低)", "type":"string"}

         |}
         |}""".stripMargin
    val spark = SparkSession.builder()
      .appName("shc test")
      .master("local[10]")
      .getOrCreate()
    import spark.implicits._
    val df= spark.read
      .option(HBaseTableCatalog.tableCatalog, catalog)
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .load().toDF()
    val births=df.select(col("age_class").as("birth"),col("exchange_item_rate(高,中,低)"))
    val s4=births.groupBy("birth","exchange_item_rate(高,中,低)")
      .agg(count("exchange_item_rate(高,中,低)")as "exchange_item_rate(高,中,低)_count")
      .sort(col("exchange_item_rate(高,中,低)_count").desc).dropDuplicates("birth")
    s4.show(false)
    val s5=s4.select(col("birth"),col("exchange_item_rate(高,中,低)").as("exchange_item_rate"))

    s5.show(false)
    def catalogWrite =
      s"""{
         |"table":{"namespace":"default", "name":"birth_colony_exchange_item_rate"},
         |"rowkey":"birth",
         |"columns":{
         |"birth":{"cf":"rowkey", "col":"birth", "type":"string"},
         |"exchange_item_rate":{"cf":"cf", "col":"exchange_item_rate", "type":"string"}
         |}
         |}""".stripMargin
    s5.write
      .option(HBaseTableCatalog.tableCatalog, catalogWrite)
      .option(HBaseTableCatalog.newTable, "5")
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .save()
  }

  def birth_colony_return_item_rate():Unit= {
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

         |"brand_preference":{"cf":"cf", "col":"brand_preference", "type":"string"},
         |"payment_way":{"cf":"cf", "col":"payment_way", "type":"string"},
         |"return_item_rate(高,中,低)":{"cf":"cf", "col":"return_item_rate(高,中,低)", "type":"string"}

         |}
         |}""".stripMargin
    val spark = SparkSession.builder()
      .appName("shc test")
      .master("local[10]")
      .getOrCreate()
    import spark.implicits._
    val df= spark.read
      .option(HBaseTableCatalog.tableCatalog, catalog)
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .load().toDF()

    val births=df.select(col("age_class").as("birth"),col("return_item_rate(高,中,低)"))
    val s4=births.groupBy("birth","return_item_rate(高,中,低)")
      .agg(count("return_item_rate(高,中,低)")as "return_item_rate(高,中,低)_count")
      .sort(col("return_item_rate(高,中,低)_count").desc).dropDuplicates("birth")
    s4.show(false)
    val s5=s4.select(col("birth"),col("return_item_rate(高,中,低)").as("return_item_rate"))
    def catalogWrite =
      s"""{
         |"table":{"namespace":"default", "name":"birth_colony_return_item_rate"},
         |"rowkey":"birth",
         |"columns":{
         |"birth":{"cf":"rowkey", "col":"birth", "type":"string"},
         |"return_item_rate":{"cf":"cf", "col":"return_item_rate", "type":"string"}
         |}
         |}""".stripMargin
    s5.write
      .option(HBaseTableCatalog.tableCatalog, catalogWrite)
      .option(HBaseTableCatalog.newTable, "5")
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .save()
  }




}
