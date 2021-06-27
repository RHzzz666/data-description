package wcl_user

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.execution.datasources.hbase.HBaseTableCatalog
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

object user_final_2 {
  def main(args: Array[String]): Unit = {
    def user_final_write =
      s"""{
         |"table":{"namespace":"default","name":"user_final"},
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
         |"log_time_arrange":{"cf":"cf", "col":"log_time_arrange", "type":"string"}
         |}
         |}""".stripMargin

    def consumption_ablity_write=
      s"""{
         |  "table":{"namespace":"default", "name":"user_class"},
         |  "rowkey":"id",
         |  "columns":{
         |    "id":{"cf":"rowkey", "col":"id", "type":"long"},
         |    "user_name":{"cf":"cf", "col":"username", "type":"string"},
         |    "consumption_ablity":{"cf":"cf", "col":"consumptionAblity", "type":"string"}
         |  }
         |}""".stripMargin

    val spark = SparkSession.builder()
      .appName("behavior_record")
      .master("local")
      .getOrCreate()

    import spark.implicits._
    val user_final_df: DataFrame = spark.read
      .option(HBaseTableCatalog.tableCatalog, user_final_write)
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .load()
    val consumption_ablity_df: DataFrame = spark.read
      .option(HBaseTableCatalog.tableCatalog, consumption_ablity_write)
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .load()
    //consumption_ablity_df.show(false)
    //合并代码
    val res = user_final_df.join(consumption_ablity_df,user_final_df.col("id")===consumption_ablity_df.col("id").cast("string"))
      .drop(consumption_ablity_df.col("id"))
      .drop(consumption_ablity_df.col("user_name"))

    res.show(false)
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

    res.write
      .option(HBaseTableCatalog.tableCatalog, user_final_2)
      .option(HBaseTableCatalog.newTable, "5")
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .save()

  }
}
