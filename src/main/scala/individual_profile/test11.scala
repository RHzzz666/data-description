package individual_profile

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.execution.datasources.hbase.HBaseTableCatalog
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame, SparkSession, functions}

//面向tbl_orders
//- 消费周期 1
//- 消费能力
//- 客单价1
//- 支付方式1
//- 单笔最高1
//- 购买频率1
//- 退货率1
//- 换货率1
object test11 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Orders_info")
      .master("local")
      .getOrCreate()
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)

    def orders_info_catalog =
      s"""{
         |"table":{"namespace":"default", "name":"tbl_orders"},
         |"rowkey":"id",
         |"columns":{
         |"id":{"cf":"rowkey", "col":"id", "type":"long"},
         |"member_id":{"cf":"cf", "col":"memberId", "type":"string"},
         |"payment_status":{"cf":"cf", "col":"paymentStatus", "type":"string"},
         |"modified":{"cf":"cf", "col":"modified", "type":"string"},
         |"payment_way":{"cf":"cf", "col":"paymentName", "type":"string"},
         |"order_status":{"cf":"cf", "col":"orderStatus", "type":"string"},
         |"orderSn":{"cf":"cf", "col":"orderSn", "type":"string"},
         |"order_amount":{"cf":"cf", "col":"orderAmount", "type":"string"},
         |"finish_time":{"cf":"cf", "col":"finishTime", "type":"string"}
         |}
         |}""".stripMargin

    //"codConfirmState":货到侍确认状态0无需未确认,1待确认,2确认通过可以发货,3确认无效,订单可以取消


    import spark.implicits._

    val df: DataFrame = spark.read
      .option(HBaseTableCatalog.tableCatalog, orders_info_catalog)
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .load()
    //注入函数
    val get_id = functions.udf(string_last_3_char _)

    val basic_df = df.withColumn("member_id", get_id('member_id))


    // 1.
    // 按照用户的支付方式的统计
    // 找出payment_way——支付方式
    val pay_way_df = basic_df
      .where('order_status === "202")
      .groupBy('member_id, 'payment_way)
      .agg(count('payment_way) as "count")
      .withColumn("row_num", row_number() over Window.partitionBy('member_id).orderBy('count.desc))
      .where('row_num === 1)
      .drop("row_num", "count")
      .select('member_id, 'payment_way)
    //    //pay_way_df.show(false)
    //    //show的结果：
    //    //+---------+-----------+-----+
    //    //|member_id|payment_way|count|
    //    //+---------+-----------+-----+
    //    //|467      |支付宝     |59   |
    //    //|675      |支付宝     |39   |
    //    //|691      |支付宝     |47   |
    //    //|125      |支付宝     |37   |
    //
    //    // 2.
    //    // 得到用户的消费周期，确定周期范围，保留周期
    val cycle_result_df = basic_df.withColumn("modified", to_timestamp('modified))
      //订单状态 202 成功
      .where('order_status === "202")
      .groupBy('member_id)
      .agg(min('modified) as "start_time_0", min('id) as "id", max('modified) as "end_time_0", count('member_id) as "order_count", countDistinct(dayofyear('modified)) as "day")
      .select('id, 'member_id, 'day, (datediff('end_time_0, 'start_time_0) + 1).as("consumption"))
      .withColumn("cycle", 'consumption / 'day)
      .select('id, 'cycle, 'member_id, when('cycle < "7", "7日")
        .when('cycle between(7, 14), "2周")
        .when('cycle between(14, 30), "1月")
        .when('cycle between(30, 60), "2月")
        .when('cycle between(60, 90), "3月")
        .when('cycle between(90, 120), "4月")
        .when('cycle between(120, 150), "5月")
        .when('cycle between(150, 180), "6月")
        .otherwise("六月以上")
        .as("shopping_cycle")).drop("cycle")
    //
    //    //cycle_result_df.show(false)
    //    //show
    ////    +-----+---------+--------------+
    ////    |id   |member_id|shopping_cycle|
    ////    +-----+---------+--------------+
    ////    |1635 |467      |7日           |
    ////    |3990 |691      |2周           |
    ////    |2355 |675      |1月           |
    //
    //
    //    // 3
    //    //按照用户计算出：每个用户平均单价
    val user_ave_price_df = basic_df
      .where('order_status === "202")
      .groupBy('member_id)
      .agg(avg('order_amount) as "ave_price")
      .select('ave_price, 'member_id, when('ave_price between(1, 499), "1-499")
        .when('ave_price between(500, 999), "500-999")
        .when('ave_price between(1000, 2999), "1000-2999")
        .when('ave_price between(3000, 4999), "3000-4999")
        .when('ave_price between(5000, 9999), "5000-9999")
        .as("ave_price_range")
      ).drop("order_amount")
    //    user_ave_price_df.show(false)
    //    //show
    //    //+------------------+---------+---------------+
    //    //|ave_price         |member_id|ave_price_range|
    //    //+------------------+---------+--------------+
    //    //|1782.9028767123289|467      |1000-2999     |
    //    //|1797.3695652173913|675      |1000-2999     |


    // 4
    //按照用户计算出：每个用户每单最高
    val order_highest_df = basic_df
      .where('order_status === "202")
      .groupBy('member_id)
      .agg(max('order_amount) as "order_highest")
      .select('order_highest, 'member_id,
        when('order_highest between(1, 499), "1-499")
          .when('order_highest between(500, 999), "500-999")
          .when('order_highest between(1000, 2999), "1000-2999")
          .when('order_highest between(3000, 4999), "3000-4999")
          .when('order_highest between(5000, 9999), "5000-9999")
          .as('order_highest_range))

    //    order_highest_df.show(false)

    //show:
    //+-------------+---------+-------------------+
    //|order_highest|member_id|order_highest_range|
    //+-------------+---------+-------------------+
    //|99.00        |467      |1-999              |
    //|998.00       |675      |1-999              |


    // 5
    //计算购物频率,计算方法：购物次数/购物天数
    val frequency_df = basic_df.withColumn("modified", to_timestamp('modified))
      .where('order_status === "202")
      .groupBy('member_id)
      .agg(min('modified) as "start_time", max('modified) as "end_time", count('member_id) as "order_count")
      .select('member_id, 'order_count, (datediff('end_time, 'start_time) + 1).as("consumption"))
      .withColumn("frequency", 'order_count / 'consumption)
      .select('frequency, 'member_id, 'consumption, 'order_count, when('frequency > "1.0", "高")
        .when('frequency between(0.6, 1.0), "中")
        .when('frequency < "0.6", "低")
        .as("frequency_range(高,中,低)"))
    //    frequency_df.show(false)
    //show的结果：
    //+-------------------+---------+-----------+-----------+---------------+
    //|frequency          |member_id|consumption|order_count|frequency_range|
    //+-------------------+---------+-----------+-----------+---------------+
    //|1.106060606060606  |467      |66         |73         |低             |
    //|0.9387755102040817 |675      |49         |46         |低             |

    //6
    //退换货率
    //未付款确认可发货视为换货，未付款确认取消视为退货
    //退换货率
    //exchange_换货
    //return 退货
    val ret_or_exc_df = basic_df
      .groupBy('member_id)
      .agg(count('member_id) as "order_count",
        sum(when('order_status === "1", 1).otherwise(0)) as "exchange_item",
        sum(when('order_status === "0", 1).otherwise(0)) as "return_item")
      .select('member_id, 'order_count, 'exchange_item, 'return_item)
      .withColumn("exchange_item_rate", 'exchange_item / 'order_count)
      .withColumn("return_item_rate", 'return_item / 'order_count)
      .select('exchange_item_rate, 'return_item_rate, 'member_id, when('exchange_item_rate > "0.05", "高")
        .when('exchange_item_rate between(0.01, 0.05), "中")
        .when('exchange_item_rate < "0.01", "低")
        .as("exchange_item_rate(高,中,低)"),
        when('return_item_rate > "0.03", "高")
          .when('return_item_rate between(0.01, 0.03), "中")
          .when('return_item_rate < "0.03", "低")
          .as("return_item_rate(高,中,低)")
      )
    //ret_or_exc_df.show(false)
    //.drop("换货率", "退货率")
    //show的结果：
    //+------------------+--------------------+---------+----------------------------+--------------------------+
    //|exchange_item_rate|return_item_rate    |member_id|exchange_item_rate(高,中,低)|return_item_rate(高,中,低)|
    //+------------------+--------------------+---------+----------------------------+--------------------------+
    //|0.0               |0.004149377593360996|675      |低                          |低                        |
    //|0.0               |0.0                 |691      |低                          |低                        |
    //|0.0               |0.0                 |467      |低                          |低                        |


    //    val colRencency = "rencency"
    //    val colFrequency = "frequency"
    //    val colMoneyTotal = "money_total"
    //    val colFeature = "feature"
    //    val colPredict = "predict"
    //    val days_range = 660
    //
    //    // 统计距离最近一次消费的时间
    //    val recencyCol = datediff(date_sub(current_timestamp(), days_range), from_unixtime(max('finish_time))) as colRencency
    //    // 统计订单总数
    //    val frequencyCol = count('orderSn) as colFrequency
    //    // 统计订单总金额
    //    val moneyTotalCol = sum('order_amount) as colMoneyTotal
    //
    //    val RFMResult = basic_df.groupBy('member_id)
    //      .agg(recencyCol, frequencyCol, moneyTotalCol)
    //2.为RFM打分
    //R: 1-3天=5分，4-6天=4分，7-9天=3分，10-15天=2分，大于16天=1分
    //F: ≥200=5分，150-199=4分，100-149=3分，50-99=2分，1-49=1分
    //M: ≥20w=5分，10-19w=4分，5-9w=3分，1-4w=2分，<1w=1分
    //    val recencyScore: Column = functions.when((col(colRencency) >= 1) && (col(colRencency) <= 3), 5)
    //      .when((col(colRencency) >= 4) && (col(colRencency) <= 6), 4)
    //      .when((col(colRencency) >= 7) && (col(colRencency) <= 9), 3)
    //      .when((col(colRencency) >= 10) && (col(colRencency) <= 15), 2)
    //      .when(col(colRencency) >= 16, 1)
    //      .as(colRencency)
    //
    //    val frequencyScore: Column = functions.when(col(colFrequency) >= 200, 5)
    //      .when((col(colFrequency) >= 150) && (col(colFrequency) <= 199), 4)
    //      .when((col(colFrequency) >= 100) && (col(colFrequency) <= 149), 3)
    //      .when((col(colFrequency) >= 50) && (col(colFrequency) <= 99), 2)
    //      .when((col(colFrequency) >= 1) && (col(colFrequency) <= 49), 1)
    //      .as(colFrequency)
    //
    //    val moneyTotalScore: Column = functions.when(col(colMoneyTotal) >= 200000, 5)
    //      .when(col(colMoneyTotal).between(100000, 199999), 4)
    //      .when(col(colMoneyTotal).between(50000, 99999), 3)
    //      .when(col(colMoneyTotal).between(10000, 49999), 2)
    //      .when(col(colMoneyTotal) <= 9999, 1)
    //      .as(colMoneyTotal)
    //
    //    val RFMScoreResult = RFMResult.select('member_id, recencyScore, frequencyScore, moneyTotalScore)
    //
    //    RFMScoreResult.show(false)
    //
    //    //    val vectorDF = new VectorAssembler()
    //    //      .setInputCols(Array(colRencency, colFrequency, colMoneyTotal))
    //    //      .setOutputCol(colFeature)
    //    //      .transform(RFMScoreResult)
    //    //
    //    //    val kmeans = new KMeans()
    //    //      .setK(7)
    //    //      .setSeed(100)
    //    //      .setMaxIter(2)
    //    //      .setFeaturesCol(colFeature)
    //    //      .setPredictionCol(colPredict)
    //    //
    //    //    // train model
    //    //    val model = kmeans.fit(vectorDF)
    //    //
    //    //    val predicted = model.transform(vectorDF)
    //    //
    //    //    val result0=predicted.select('memberId,'predict,when('predict==="0","超高")
    //    //      .when('predict==="1","高")
    //    //      .when('predict==="2","中上")
    //    //      .when('predict==="3","中")
    //    //      .when('predict==="4","中下")
    //    //      .when('predict==="5","低")
    //    //      .when('predict==="6","很低")as("用户价值")).drop("rencency","frequency","moneyTotal","feature","predict")
    //        //消费能力
    //
    //
    //
    //    //此处是将之前的所有DataFrame 集合在一起
    val result0: DataFrame = pay_way_df.join(cycle_result_df, pay_way_df.col("member_id") === cycle_result_df("member_id"))
      .drop(cycle_result_df.col("member_id"))
    val result1: DataFrame = result0.join(user_ave_price_df, user_ave_price_df.col("member_id") === result0.col("member_id"))
      .drop(user_ave_price_df.col("member_id"))
    val result2: DataFrame = result1.join(order_highest_df, order_highest_df.col("member_id") === result1.col("member_id"))
      .drop(order_highest_df.col("member_id"))
    val result3: DataFrame = result2.join(frequency_df, frequency_df.col("member_id") === result2.col("member_id"))
      .drop(frequency_df.col("member_id"))
    val result4: DataFrame = result3.join(ret_or_exc_df, ret_or_exc_df.col("member_id") === result3.col("member_id"))
      .drop(ret_or_exc_df.col("member_id"))

    result4.show(false)

    //show
    //+-----------+-----+---------+--------------+------------------+---------------+-------------+-------------------+-------------------+-----------+-----------+-------------------------+------------------+--------------------+----------------------------+--------------------------+
    //|payment_way|id   |member_id|shopping_cycle|ave_price         |ave_price_range|order_highest|order_highest_range|frequency          |consumption|order_count|frequency_range(高,中,低)|exchange_item_rate|return_item_rate    |exchange_item_rate(高,中,低)|return_item_rate(高,中,低)|
    //+-----------+-----+---------+--------------+------------------+---------------+-------------+-------------------+-------------------+-----------+-----------+-------------------------+------------------+--------------------+----------------------------+--------------------------+
    //|支付宝     |1635 |467      |7日           |1782.9028767123289|1000-2999      |99.00        |1-999              |1.106060606060606  |66         |73         |低                       |0.0               |0.0                 |低                          |低                        |
    //|支付宝     |2355 |675      |1月           |1797.3695652173913|1000-2999      |998.00       |1-999              |0.9387755102040817 |49         |46         |低                       |0.0               |0.004149377593360996|低                          |低                        |
    //|支付宝     |3990 |691      |2周           |1994.7226785714286|1000-2999      |99.00        |1-999              |0.8115942028985508 |69         |56         |低                       |0.0               |0.0                 |低                          |低                        |
    def orders_info_write_catalog =
      s"""{
         |"table":{"namespace":"default", "name":"user_orders_info"},
         |"rowkey":"id",
         |"columns":{
         |"id":{"cf":"rowkey", "col":"id", "type":"long"},
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

    result4.write
      .option(HBaseTableCatalog.tableCatalog, orders_info_write_catalog)
      .option(HBaseTableCatalog.newTable, "5")
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .save()

    //    //    val result7: DataFrame=result.join(result2,result.col("memberId")===result2.col("memberId"))
    //    //      .drop(result2.col("memberId"))
    //    //    val result8: DataFrame=result7.join(result3,result7.col("memberId")===result3.col("memberId"))
    //    //      .drop(result3.col("memberId"))
    //    //    val result9: DataFrame=result8.join(result4,result8.col("memberId")===result4.col("memberId"))
    //    //      .drop(result4.col("memberId"))
    //    //    val result10: DataFrame=result9.join(result5,result9.col("memberId")===result5.col("memberId"))
    //    //      .drop(result5.col("memberId"))
    //    //    val result11: DataFrame=result10.join(result6,result10.col("memberId")===result6.col("memberId"))
    //    //      .drop(result6.col("memberId"))
    //    //    val result12: DataFrame=result11.join(result0,result11.col("memberId")===result0.col("memberId"))
    //    //      .drop(result0.col("memberId"))
    //    //
    //    //
    //    //points（积分）100：1,coupondCodeValue(优惠码)、giftCordAmounnt（活动卡，理解为券来分析有券必买）1：1
    //    def catalogWrite =
    //      s"""{
    //         |"table":{"namespace":"default", "name":"consumer"},
    //         |"rowkey":"id",
    //         |"columns":{
    //         |"id":{"cf":"rowkey", "col":"id", "type":"long"},
    //         |"memberId":{"cf":"cf", "col":"memberId", "type":"string"},
    //         |"周期":{"cf":"cf", "col":"周期", "type":"string"},
    //         |"paymentName":{"cf":"cf", "col":"paymentName", "type":"string"},
    //         |"客户平均每单价格":{"cf":"cf", "col":"客户平均每单价格", "type":"string"},
    //         |"单笔最高":{"cf":"cf", "col":"单笔最高", "type":"string"},
    //         |"频率":{"cf":"cf", "col":"频率", "type":"string"},
    //         |"换货率（高、中、低）":{"cf":"cf", "col":"换货率（高、中、低）", "type":"string"},
    //         |"退货率（高、中、低）":{"cf":"cf", "col":"退货率（高、中、低）", "type":"string"},
    //         |"用户价值":{"cf":"cf", "col":"用户价值", "type":"string"}
    //         |}
    //         |}""".stripMargin

    //    result12.write
    //      .option(HBaseTableCatalog.tableCatalog, catalogWrite)
    //      .option(HBaseTableCatalog.newTable, "5")
    //      .format("org.apache.spark.sql.execution.datasources.hbase")
    //      .save()
  }

  def string_last_3_char(str: String): String = {
    val len = str.length
    if (len > 3) str.substring(len - 3, len) else str
  }
}
