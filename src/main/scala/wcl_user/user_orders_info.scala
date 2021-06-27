package wcl_user

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.execution.datasources.hbase.HBaseTableCatalog
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession, functions}

//面向tbl_orders
//- 消费周期 1
//- 消费能力
//- 客单价1
//- 支付方式1
//- 单笔最高1
//- 购买频率1
//- 退货率1
//- 换货率1
object user_orders_info {
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

    //double数据类型
    //ave_price , order_highest, frequency ,exchange_item_rate, return_item_rate
    def orders_info_write_catalog=
      s"""{
         |"table":{"namespace":"default", "name":"user_orders_info"},
         |"rowkey":"id",
         |"columns":{
         |"id":{"cf":"rowkey", "col":"id", "type":"long"},
         |"member_id":{"cf":"cf", "col":"member_id", "type":"string"},
         |"payment_way":{"cf":"cf", "col":"payment_way", "type":"string"},
         |"shopping_cycle":{"cf":"cf", "col":"shopping_cycle", "type":"string"},
         |"ave_price":{"cf":"cf", "col":"ave_price", "type":"double"},
         |"ave_price_range":{"cf":"cf", "col":"ave_price_range", "type":"string"},
         |"order_count":{"cf":"cf", "col":"order_count","type":"long"},
         |"order_highest":{"cf":"cf", "col":"order_highest", "type":"double"},
         |"order_highest_range":{"cf":"cf", "col":"order_highest_range", "type":"string"},
         |"frequency":{"cf":"cf", "col":"frequency", "type":"double"},
         |"frequency_range(高,中,低)":{"cf":"cf", "col":"frequency_range(高,中,低)", "type":"string"},
         |"exchange_item_rate":{"cf":"cf", "col":"exchange_item_rate", "type":"double"},
         |"return_item_rate":{"cf":"cf", "col":"return_item_rate", "type":"double"},
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


  }


  def string_last_3_char(str: String): String = {
    val len = str.length
    val sub_str = if (len > 3) str.substring(len - 3, len) else str
    if (sub_str(0) == '0' && sub_str(1) == '0') sub_str.substring(2)
    else if (sub_str(0) == '0') sub_str.substring(1, 3)
    else sub_str
  }
}
