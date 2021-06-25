package individual_profile

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.execution.datasources.hbase.HBaseTableCatalog
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession, functions}


object test222 {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    def user_basic_info_write_catalog =
      s"""{
         |"table":{"namespace":"default","name":"user_basic_info"},
         |"rowkey":"id",
         |"columns":{
         |"id":{"cf":"rowkey","col":"id","type":"string"},
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
         |"constellation":{"cf":"cf","col":"constellation","type":"string"}
         |}
         |}
       """.stripMargin

    def goods_catalog_write =
      s"""{
         |"table":{"namespace":"default", "name":"user_goods_and_order"},
         |"rowkey":"member_id",
         |"columns":{
         |"member_id":{"cf":"rowkey", "col":"member_id", "type":"string"},
         |"scanned_goods":{"cf":"cf", "col":"scanned_goods", "type":"string"},
         |"product_type":{"cf":"cf", "col":"product_type", "type":"string"},
         |"good_bought":{"cf":"cf", "col":"good_bought", "type":"string"},
         |"brand_preference":{"cf":"cf", "col":"brand_preference", "type":"string"},
         |"product_name":{"cf":"cf", "col":"productName", "type":"string"}
         |}
         |}""".stripMargin

    def orders_info_write_catalog =
      s"""{
         |"table":{"namespace":"default", "name":"user_orders_info"},
         |"rowkey":"id",
         |"columns":{
         |"not_id":{"cf":"rowkey", "col":"id", "type":"string"},
         |"member_id":{"cf":"cf", "col":"member_id", "type":"string"},
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
         |"consumption":{"cf":"cf", "col":"consumption", "type":"string"}
         |}
         |}""".stripMargin

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

    val spark = SparkSession.builder()
      .appName("behavior_record")
      .master("local")
      .getOrCreate()

    val user_basic_info_df: DataFrame = spark.read
      .option(HBaseTableCatalog.tableCatalog, user_basic_info_write_catalog)
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .load()
   // user_basic_info_df.show(false)
    val goods_catalog_df: DataFrame = spark.read
      .option(HBaseTableCatalog.tableCatalog, goods_catalog_write)
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .load()
    goods_catalog_df.show(false)
    val orders_info_catalog_df:DataFrame = spark.read
      .option(HBaseTableCatalog.tableCatalog, orders_info_write_catalog)
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .load()

    val behavior_record_df:DataFrame = spark.read
      .option(HBaseTableCatalog.tableCatalog, behavior_record_write)
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .load()

    var res1=user_basic_info_df.join(goods_catalog_df,user_basic_info_df.col("id")===goods_catalog_df("member_id"))
      .drop(goods_catalog_df("member_id"))
//    res1.show(false)
//    var res2=orders_info_catalog_df.join(behavior_record_df,behavior_record_df.col("global_user_id")===orders_info_catalog_df.col("member_id"))
//      .drop(orders_info_catalog_df.col("member_id"))
//    res2.show(false)
//
//    var res3=res1.join(res2,res1.col("id")===res2.col("global_user_id"))
//      .drop(res2.col("global_user_id"))
//    res3.show(false)

//    def user_final_write =
//      s"""{
//         |"table":{"namespace":"default","name":"user_final"},
//         |"rowkey":"id",
//         |"columns":{
//         |"id":{"cf":"rowkey","col":"id","type":"long"},
//         |"e_mail":{"cf":"cf","col":"email","type":"string"},
//         |"user_name":{"cf":"cf","col":"username","type":"string"},
//         |"password":{"cf":"cf","col":"password","type":"string"},
//         |"birthday":{"cf":"cf","col":"birthday","type":"string"},
//         |"mobile":{"cf":"cf","col":"mobile","type":"string"},
//         |"money":{"cf":"cf","col":"money","type":"string"},
//         |"money_pwd":{"cf":"cf","col":"money_pwd","type":"string"},
//         |"nick_name":{"cf":"cf","col":"nick_name","type":"string"},
//         |"last_login_time":{"cf":"cf","col":"last_login_time","type":"string"},
//         |"register_time":{"cf":"cf","col":"register_time","type":"string"},
//         |"qq":{"cf":"cf","col":"qq","type":"string"},
//         |"job":{"cf":"cf","col":"job","type":"string"},
//         |"political_face":{"cf":"cf","col":"political_face","type":"string"},
//         |"age_class":{"cf":"cf","col":"age_class","type":"string"},
//         |"nationality":{"cf":"cf","col":"nationality","type":"string"},
//         |"marriage":{"cf":"cf","col":"marriage","type":"string"},
//         |"is_in_blacklist":{"cf":"cf","col":"is_in_blacklist","type":"string"},
//         |"constellation":{"cf":"cf","col":"constellation","type":"string"},
//
//         |"scanned_goods":{"cf":"cf", "col":"scanned_goods", "type":"string"},
//         |"product_type":{"cf":"cf", "col":"product_type", "type":"string"},
//         |"good_bought":{"cf":"cf", "col":"good_bought", "type":"string"},
//         |"brand_preference":{"cf":"cf", "col":"brand_preference", "type":"string"},
//         |"product_name":{"cf":"cf", "col":"productName", "type":"string"},
//
//         |"payment_way":{"cf":"cf", "col":"payment_way", "type":"string"},
//         |"shopping_cycle":{"cf":"cf", "col":"shopping_cycle", "type":"string"},
//         |"ave_price":{"cf":"cf", "col":"ave_price", "type":"string"},
//         |"ave_price_range":{"cf":"cf", "col":"ave_price_range", "type":"string"},
//         |"order_count":{"cf":"cf", "col":"order_count","type":"long"},
//         |"order_highest":{"cf":"cf", "col":"order_highest", "type":"string"},
//         |"order_highest_range":{"cf":"cf", "col":"order_highest_range", "type":"string"},
//         |"frequency":{"cf":"cf", "col":"frequency", "type":"string"},
//         |"frequency_range(高,中,低)":{"cf":"cf", "col":"frequency_range(高,中,低)", "type":"string"},
//         |"exchange_item_rate":{"cf":"cf", "col":"exchange_item_rate", "type":"string"},
//         |"return_item_rate":{"cf":"cf", "col":"return_item_rate", "type":"string"},
//         |"exchange_item_rate(高,中,低)":{"cf":"cf", "col":"exchange_item_rate(高,中,低)", "type":"string"},
//         |"return_item_rate(高,中,低)":{"cf":"cf", "col":"return_item_rate(高,中,低)", "type":"string"},
//         |"consumption":{"cf":"cf", "col":"consumption", "type":"string"},
//
//         |"scanned_page":{"cf":"cf", "col":"scanned_page", "type":"string"},
//         |"scan_total_time":{"cf":"cf", "col":"scan_total_time", "type":"string"},
//         |"count_scan":{"cf":"cf", "col":"count_scan", "type":"string"},
//         |"log_frequency":{"cf":"cf", "col":"log_frequency", "type":"string"},
//         |"log_time":{"cf":"cf", "col":"log_time", "type":"string"},
//         |"count_log":{"cf":"cf", "col":"count_log", "type":"string"},
//         |"log_time_arrange":{"cf":"cf", "col":"log_time_arrange", "type":"string"}
//         |}
//         |}""".stripMargin
//
//    res3.write
//      .option(HBaseTableCatalog.tableCatalog, user_final_write)
//      .option(HBaseTableCatalog.newTable, "5")
//      .format("org.apache.spark.sql.execution.datasources.hbase")
//      .save()
  }
}
