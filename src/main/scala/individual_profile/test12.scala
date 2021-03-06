package individual_profile

import org.apache.spark.sql.execution.datasources.hbase.HBaseTableCatalog
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession, functions}


object test12 {
  def main(args: Array[String]): Unit = {
    def catalog =
      s"""{
         |"table":{"namespace":"default", "name":"tbl_users"},
         |"rowkey":"id",
         |"columns":{
         |"id":{"cf":"rowkey", "col":"id", "type":"long"},
         |"username":{"cf":"cf", "col":"username", "type":"string"},
         |"gender":{"cf":"cf", "col":"gender", "type":"string"},
         |"e_mail":{"cf":"cf", "col":"email", "type":"string"},
         |"mobile":{"cf":"cf", "col":"mobile", "type":"string"},
         |"job":{"cf":"cf", "col":"job", "type":"string"},
         |"political_face":{"cf":"cf", "col":"politicalFace", "type":"string"},
         |"nationality":{"cf":"cf", "col":"nationality", "type":"string"},
         |"marriage":{"cf":"cf", "col":"marriage", "type":"string"},
         |"birthday":{"cf":"cf", "col":"birthday", "type":"string"},
         |"modified":{"cf":"cf", "col":"modified", "type":"string"},
         |"register_time":{"cf":"cf", "col":"registerTime", "type":"string"},
         |"last_login_time":{"cf":"cf", "col":"lastLoginTime", "type":"string"},
         |"is_blackList":{"cf":"cf", "col":"is_blackList", "type":"string"}
         |}
         |}""".stripMargin
    def user_log_catalog =
      s"""{
         |"table":{"namespace":"default", "name":"tbl_logs"},
         |"rowkey":"id",
         |"columns":{
         |"id":{"cf":"rowkey", "col":"id", "type":"long"},
         |"global_user_id":{"cf":"cf", "col":"global_user_id", "type":"string"},
         |"user_agent":{"cf":"cf", "col":"user_agent", "type":"string"},
         |"loc_url":{"cf":"cf", "col":"loc_url", "type":"string"},
         |"log_time":{"cf":"cf", "col":"log_time", "type":"string"}
         |}
         |}""".stripMargin
    def catalog_order =
      s"""{
         |"table":{"namespace":"default", "name":"tbl_orders"},
         |"rowkey":"id",
         |"columns":{
         |"id":{"cf":"rowkey", "col":"id", "type":"long"},
         |"member_id":{"cf":"cf", "col":"member_id", "type":"string"},
         |"payment_status":{"cf":"cf", "col":"payment_status", "type":"string"},
         |"modified":{"cf":"cf", "col":"modified", "type":"string"},
         |"payment_way":{"cf":"cf", "col":"payment_way", "type":"string"},
         |"order_status":{"cf":"cf", "col":"order_status", "type":"string"},
         |"orderSn":{"cf":"cf", "col":"orderSn", "type":"string"},
         |"order_amount":{"cf":"cf", "col":"order_amount", "type":"string"},
         |"finish_time":{"cf":"cf", "col":"finish_time", "type":"string"}
         |}
         |}""".stripMargin
    def catalog_goods =
      s"""{
         |"table":{"namespace":"default", "name":"tbl_goods"},
         |"rowkey":"id",
         |"columns":{
         |"id":{"cf":"rowkey", "col":"id", "type":"long"},
         |"c_orderSn":{"cf":"cf", "col":"cOrderSn", "type":"string"},
         |"product_type":{"cf":"cf", "col":"productType", "type":"string"},
         |"product_name":{"cf":"cf", "col":"productName", "type":"string"}
         |}
         |}""".stripMargin

    val spark = SparkSession.builder()
      .appName("good_behavior")
      .master("local")
      .getOrCreate()

    import spark.implicits._
    val get_id = functions.udf(string_last_3_char _)

    //???????????????????????????
    val user_info_df: DataFrame = spark.read
      .option(HBaseTableCatalog.tableCatalog, catalog)
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .load()
    val orders_df: DataFrame = spark.read
      .option(HBaseTableCatalog.tableCatalog, catalog_order)
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .load()
    val goods_df: DataFrame = spark.read
      .option(HBaseTableCatalog.tableCatalog, catalog_goods)
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .load()
    
    val good_order:DataFrame = goods_df.join(orders_df,orders_df.col("orderSn")===goods_df.col("c_orderSn"))
      .withColumn("member_id",get_id('member_id))
    //1.
    //???????????????
    val scanned_goods = good_order.select('member_id,'product_name).groupBy("member_id")
      .agg(collect_set('product_name) as "scanned_goods")

    scanned_goods.show(false)
    
    //2.
    //??????????????????????????????????????????
//    val b = good_order.select('member_id,'product_name,'order_status,'product_type)
//      .where('order_status==="202")
//      .groupBy("member_id","product_type")
//      .agg(collect_set('product_name) as "????????????",count("product_type") as "count")
//      .withColumn("row_num", row_number() over Window.partitionBy('member_id).orderBy('count.desc))
//      .where('row_num === 1)
//      .drop("count", "row_num")
//    //???????????????????????????
//    val c = good_order.select('member_id,'product_name,'order_status)
//      .where('order_status==="202")
//      .groupBy("member_id","product_name")
//      .agg(count("product_name") as "count")
//      .withColumn("row_num", row_number() over Window.partitionBy('member_id).orderBy('count.desc))
//      .where('row_num === 1)
//      .drop("count", "row_num")
//    //
//    //????????????????????????????????????????????????
//    val d = good_order.select('member_id,'product_name,'order_status,
//      when('product_name like "%?????????%","?????????")
//        .when('product_name like "%MOOKA%","??????")
//        .when('product_name like "%KFR%","?????????")
//        .when('product_name like "%??????%","??????")
//        .when('product_name like "%??????%","??????")
//        .otherwise("??????")
//        .as("brand_preference"))
//      .where('order_status==="202")
//      .groupBy("member_id","brand_preference")
//      .agg(sum(when('brand_preference==="??????",1).when('brand_preference==="??????",1).otherwise(10)) as "count")
//      .withColumn("row_num", row_number() over Window.partitionBy('member_id).orderBy('count.desc))
//      .where('row_num === 1)
//      .drop("count", "row_num")
//    //???????????????????????????????????????????????????????????????
//    val result1: DataFrame=scanned_goods.join(b,scanned_goods.col("member_id")===b.col("member_id"))
//      .drop(b.col("member_id"))
//    val result2: DataFrame=result1.join(c,result1.col("member_id")===c.col("member_id"))
//      .drop(c.col("member_id"))
//    val result3: DataFrame=result2.join(d,result2.col("member_id")===d.col("member_id"))
//      .drop(d.col("member_id"))
//
//
//    //??????
////    val fplace = functions.udf(place _)
////    val fplaces = functions.udf(places _)
//    //???????????????
//    val log_DF: DataFrame = spark.read
//      .option(HBaseTableCatalog.tableCatalog, user_log_catalog)
//      .format("org.apache.spark.sql.execution.datasources.hbase")
//      .load()
//    //??????????????????????????????????????????????????????
//    val logDF_pre = log_DF.drop('id)
//      .select('global_user_id,'user_agent,'loc_url,'log_time,
//        when('loc_url like("%login%"),"?????????")
//          .when('loc_url like("%order%"),"???????????????")
//          .when('loc_url like("%product%"),"?????????")
//          .when('loc_url like("%item%"),"?????????")
//          .when('loc_url like("%index%"),"??????")
//          .otherwise("????????????")
//          .as("????????????"),
//        when(hour('log_time) between(1,7),"1???-7???")
//          .when(hour('log_time) between(8,12),"8???-12???")
//          .when(hour('log_time) between(13,17),"13???-17???")
//          .when(hour('log_time) between(18,21),"18???-21???")
//          .when(hour('log_time) between(22,24),"22???-24???")
//          .as("????????????"),
//        when('user_agent like("%Android%"),"Android")
//          .when('user_agent like("%iPhone%"),"IOS")
//          .when('user_agent like("%WOW%"),"Window")
//          .when('user_agent like("%Linux%"),"Linux")
//          .otherwise("Mac")
//          .as("????????????")
//
//      )
//      .withColumn("pre_time", lag('log_time,1) over Window.partitionBy('global_user_id).orderBy('log_time.desc))
//      .withColumn("????????????",when(isnull('pre_time),"0").when((unix_timestamp('pre_time)-unix_timestamp('log_time))<60,"1?????????")
//        .when((unix_timestamp('pre_time)-unix_timestamp('log_time)) between(60,300),"1-5??????")
//        .when((unix_timestamp('pre_time)-unix_timestamp('log_time))>300,"5????????????"))
//    //????????????????????????????????????????????????
//    val log_scan = logDF_pre.groupBy('global_user_id,'????????????).agg(datediff(max('log_time),min('log_time)) as "???????????????",count('????????????) as ("count_scan"))
//      .withColumn("????????????",when('count_scan/'???????????????>1.5,"??????").
//        when('count_scan/'??????????????? between(1,1.5),"??????").
//        when('count_scan/'??????????????? between(0,1.5),"??????")
//        when('count_scan/'???????????????===0,"??????"))
//    // .groupBy('global_user_id).agg(collect_set('????????????) as("????????????"),collect_set('????????????) as("'????????????"))
//
//
//    val logDF = logDF_pre.groupBy('global_user_id).agg(max('log_time) as "log_time",
//      sum(when('????????????==="?????????",1).otherwise(0))as("count_log"))
//    //?????????????????????
//    val readDF:DataFrame = user_info_df.join(logDF,user_info_df.col("id")===logDF.col("global_user_id"))
//      .drop(logDF.col("global_user_id"))

    //???????????????
    //    val user_alys:DataFrame =   result.join(result3,result.col("id")===result3.col("member_id"))
    //      .drop(result3.col("member_id"))
    //      .withColumnRenamed("????????????","scan_product")
    //      .withColumnRenamed("????????????","buy_product")
    //      .withColumnRenamed("????????????","recent_log")
    //      .withColumnRenamed("????????????","F_log")
//    def catalogWrite =
//      s"""{
//         |"table":{"namespace":"default", "name":"user"},
//         |"rowkey":"id",
//         |"columns":{
//         |"id":{"cf":"rowkey", "col":"id", "type":"long"},
//         |"username":{"cf":"cf", "col":"username", "type":"string"},
//         |"gender":{"cf":"cf", "col":"gender", "type":"string"},
//         |"e_mail":{"cf":"cf", "col":"e_mail", "type":"string"},
//         |"mobile":{"cf":"cf", "col":"mobile", "type":"string"},
//         |"province":{"cf":"cf", "col":"province", "type":"string"},
//         |"store":{"cf":"cf", "col":"store", "type":"string"},
//         |"birthday":{"cf":"cf", "col":"birthday", "type":"string"},
//         |"job":{"cf":"cf", "col":"job", "type":"string"},
//         |"political_face":{"cf":"cf", "col":"political_face", "type":"string"},
//         |"nationality":{"cf":"cf", "col":"nationality", "type":"string"},
//         |"marriage":{"cf":"cf", "col":"marriage", "type":"string"},
//         |"age":{"cf":"cf", "col":"age", "type":"string"},
//         |"constellation":{"cf":"cf", "col":"constellation", "type":"string"},
//         |"is_blackList":{"cf":"cf", "col":"is_blackList", "type":"string"}
//         |}
//         |}""".stripMargin

//    result.write
//      .option(HBaseTableCatalog.tableCatalog, catalogWrite)
//      .option(HBaseTableCatalog.newTable, "5")
//      .format("org.apache.spark.sql.execution.datasources.hbase")
//      .save()
  }
  def string_last_3_char(str:String):String = {
    val len = str.length
    val sub_str = if(len > 3) str.substring(len-3,len) else str
    if(sub_str(0)=='0' && sub_str(1)=='0') sub_str.substring(2)
    else if(sub_str(0)=='0') sub_str.substring(1,3)
    else sub_str
  }
}
