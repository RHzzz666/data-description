package individual_profile

import org.apache.spark.sql.execution.datasources.hbase.HBaseTableCatalog
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession, functions}


object test03 {
  def main(args: Array[String]): Unit = {
    def catalog =
      s"""{
         |"table":{"namespace":"default", "name":"tbl_users"},
         |"rowkey":"id",
         |"columns":{
         |"id":{"cf":"rowkey", "col":"id", "type":"long"},
         |"username":{"cf":"cf", "col":"username", "type":"string"},
         |"gender":{"cf":"cf", "col":"gender", "type":"string"},
         |"email":{"cf":"cf", "col":"email", "type":"string"},
         |"mobile":{"cf":"cf", "col":"mobile", "type":"string"},
         |"job":{"cf":"cf", "col":"job", "type":"string"},
         |"politicalFace":{"cf":"cf", "col":"politicalFace", "type":"string"},
         |"nationality":{"cf":"cf", "col":"nationality", "type":"string"},
         |"marriage":{"cf":"cf", "col":"marriage", "type":"string"},
         |"birthday":{"cf":"cf", "col":"birthday", "type":"string"},
         |"modified":{"cf":"cf", "col":"modified", "type":"string"},
         |"registerTime":{"cf":"cf", "col":"registerTime", "type":"string"},
         |"lastLoginTime":{"cf":"cf", "col":"lastLoginTime", "type":"string"},
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
         |"memberId":{"cf":"cf", "col":"memberId", "type":"string"},
         |"paymentStatus":{"cf":"cf", "col":"paymentStatus", "type":"string"},
         |"modified":{"cf":"cf", "col":"modified", "type":"string"},
         |"paymentName":{"cf":"cf", "col":"paymentName", "type":"string"},
         |"orderStatus":{"cf":"cf", "col":"orderStatus", "type":"string"},
         |"orderSn":{"cf":"cf", "col":"orderSn", "type":"string"},
         |"orderAmount":{"cf":"cf", "col":"orderAmount", "type":"string"},
         |"finishTime":{"cf":"cf", "col":"finishTime", "type":"string"}
         |}
         |}""".stripMargin
    def catalog_goods =
      s"""{
         |"table":{"namespace":"default", "name":"tbl_goods"},
         |"rowkey":"id",
         |"columns":{
         |"id":{"cf":"rowkey", "col":"id", "type":"long"},
         |"cOrderSn":{"cf":"cf", "col":"cOrderSn", "type":"string"},
         |"productType":{"cf":"cf", "col":"productType", "type":"string"},
         |"productName":{"cf":"cf", "col":"productName", "type":"string"}
         |}
         |}""".stripMargin

    val spark = SparkSession.builder()
      .appName("behavior record")
      .master("local")
      .getOrCreate()

    import spark.implicits._
    val get_id = functions.udf(string_last_3_char _)

    //分别载入多张数据表
    val readDF0: DataFrame = spark.read
      .option(HBaseTableCatalog.tableCatalog, catalog)
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .load()
    val order_DF: DataFrame = spark.read
      .option(HBaseTableCatalog.tableCatalog, catalog_order)
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .load()
    val goods_DF: DataFrame = spark.read
      .option(HBaseTableCatalog.tableCatalog, catalog_goods)
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .load()


    val good_t = goods_DF.select('cOrderSn,'productName)
    val order_t= order_DF.select('orderSn,'memberId,'orderStatus)
    val good_order:DataFrame = goods_DF.join(order_DF,order_DF.col("orderSn")===goods_DF.col("cOrderSn"))
      .withColumn("memberId",get_id('memberId))

    //浏览的商品
    val a = good_order.select('memberId,'productName).groupBy("memberId")
      .agg(collect_set('productName)as("浏览商品"))

    //用户购买商品与最多的商品种类
    val b = good_order.select('memberId,'productName,'orderStatus,'productType)
      .where('orderStatus==="202")
      .groupBy("memberId","productType")
      .agg(collect_set('productName)as("购买商品"),count("productType") as "count")
      .withColumn("row_num", row_number() over Window.partitionBy('memberId).orderBy('count.desc))
      .where('row_num === 1)
      .drop("count", "row_num")
    //用户购买最多的商品
    val c = good_order.select('memberId,'productName,'orderStatus)
      .where('orderStatus==="202")
      .groupBy("memberId","productName")
      .agg(count("productName") as "count")
      .withColumn("row_num", row_number() over Window.partitionBy('memberId).orderBy('count.desc))
      .where('row_num === 1)
      .drop("count", "row_num")
    //
    //偏好的品牌，所给品牌皆为海尔旗下
    val d = good_order.select('memberId,'productName,'orderStatus,
      when('productName like "%卡萨帝%","卡萨帝")
        .when('productName like "%MOOKA%","摩卡")
        .when('productName like "%KFR%","小超人")
        .when('productName like "%统帅%","统帅")
        .when('productName like "%海尔%","海尔")
        .otherwise("其他")
        .as("BrandPreference"))
      .where('orderStatus==="202")
      .groupBy("memberId","BrandPreference")
      .agg(sum(when('BrandPreference==="其他",1).when('BrandPreference==="海尔",1).otherwise(10)) as "count")
      .withColumn("row_num", row_number() over Window.partitionBy('memberId).orderBy('count.desc))
      .where('row_num === 1)
      .drop("count", "row_num")
    //用户浏览与购买的商品连接，用户三偏好的连接
    val result1: DataFrame=a.join(b,a.col("memberId")===b.col("memberId"))
      .drop(b.col("memberId"))
    val result2: DataFrame=result1.join(c,result1.col("memberId")===c.col("memberId"))
      .drop(c.col("memberId"))
    val result3: DataFrame=result2.join(d,result2.col("memberId")===d.col("memberId"))
      .drop(d.col("memberId"))

    //函数
    //读取登陆表
    val log_DF: DataFrame = spark.read
      .option(HBaseTableCatalog.tableCatalog, user_log_catalog)
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .load()
    //浏览记录分析，目前暂无完全合并的需求
    val logDF_pre = log_DF.drop('id)
      .select('global_user_id,'user_agent,'loc_url,'log_time,
        when('loc_url like("%login%"),"登录页")
          .when('loc_url like("%order%"),"我的订单页")
          .when('loc_url like("%product%"),"商品页")
          .when('loc_url like("%item%"),"分类页")
          .when('loc_url like("%index%"),"首页")
          .otherwise("其他页面")
          .as("浏览页面"),
        when(hour('log_time) between(1,7),"1点-7点")
          .when(hour('log_time) between(8,12),"8点-12点")
          .when(hour('log_time) between(13,17),"13点-17点")
          .when(hour('log_time) between(18,21),"18点-21点")
          .when(hour('log_time) between(22,24),"22点-24点")
          .as("浏览时段"),
        when('user_agent like("%Android%"),"Android")
          .when('user_agent like("%iPhone%"),"IOS")
          .when('user_agent like("%WOW%"),"Window")
          .when('user_agent like("%Linux%"),"Linux")
          .otherwise("Mac")
          .as("设备类型")

      )
      .withColumn("pre_time", lag('log_time,1) over Window.partitionBy('global_user_id).orderBy('log_time.desc))
      .withColumn("浏览时间",when(isnull('pre_time),"0").when((unix_timestamp('pre_time)-unix_timestamp('log_time))<60,"1分钟内")
        .when((unix_timestamp('pre_time)-unix_timestamp('log_time)) between(60,300),"1-5分钟")
        .when((unix_timestamp('pre_time)-unix_timestamp('log_time))>300,"5分钟以上"))
    //访问频率，目前暂无完全合并的需求
    val log_scan = logDF_pre.groupBy('global_user_id,'浏览页面).agg(datediff(max('log_time),min('log_time)) as "浏览总时间",count('浏览页面) as ("count_scan"))
      .withColumn("访问频率",when('count_scan/'浏览总时间>1.5,"经常").
        when('count_scan/'浏览总时间 between(1,1.5),"很少").
        when('count_scan/'浏览总时间 between(0,1.5),"偶尔")
        when('count_scan/'浏览总时间===0,"从不"))
    // .groupBy('global_user_id).agg(collect_set('浏览页面) as("浏览页面"),collect_set('访问频率) as("'访问频率"))


    val logDF = logDF_pre.groupBy('global_user_id).agg(max('log_time) as "log_time",
      sum(when('浏览页面==="登录页",1).otherwise(0))as("count_log"))
    //合并用户与登录
    val readDF:DataFrame = readDF0.join(logDF,readDF0.col("id")===logDF.col("global_user_id"))
      .drop(logDF.col("global_user_id"))

    //尝试连接表
    //    val user_alys:DataFrame = result.join(result3,result.col("id")===result3.col("memberId"))
    //      .drop(result3.col("memberId"))
    //      .withColumnRenamed("浏览商品","scan_product")
    //      .withColumnRenamed("购买商品","buy_product")
    //      .withColumnRenamed("最近登录","recent_log")
    //      .withColumnRenamed("登陆频率","F_log")
    def catalogWrite =
      s"""{
         |"table":{"namespace":"default", "name":"user"},
         |"rowkey":"id",
         |"columns":{
         |"id":{"cf":"rowkey", "col":"id", "type":"long"},
         |"username":{"cf":"cf", "col":"username", "type":"string"},
         |"gender":{"cf":"cf", "col":"gender", "type":"string"},
         |"email":{"cf":"cf", "col":"email", "type":"string"},
         |"mobile":{"cf":"cf", "col":"mobile", "type":"string"},
         |"province":{"cf":"cf", "col":"province", "type":"string"},
         |"store":{"cf":"cf", "col":"store", "type":"string"},
         |"birthday":{"cf":"cf", "col":"birthday", "type":"string"},
         |"job":{"cf":"cf", "col":"job", "type":"string"},
         |"politicalFace":{"cf":"cf", "col":"politicalFace", "type":"string"},
         |"nationality":{"cf":"cf", "col":"nationality", "type":"string"},
         |"marriage":{"cf":"cf", "col":"marriage", "type":"string"},
         |"age":{"cf":"cf", "col":"age", "type":"string"},
         |"Constellation":{"cf":"cf", "col":"Constellation", "type":"string"},
         |"is_blackList":{"cf":"cf", "col":"is_blackList", "type":"string"}
         |}
         |}""".stripMargin

    //    result.write
    //      .option(HBaseTableCatalog.tableCatalog, catalogWrite)
    //      .option(HBaseTableCatalog.newTable, "5")
    //      .format("org.apache.spark.sql.execution.datasources.hbase")
    //      .save()
  }

  def string_last_3_char(str:String):String = {
    val len = str.length
    if(len > 3) str.substring(len-3,len)
    else str
  }
}
