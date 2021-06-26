package individual_profile

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.execution.datasources.hbase.HBaseTableCatalog
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession, functions}


object user_merge_final {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    def user_basic_info_write_catalog =
      s"""{
         |"table":{"namespace":"default","name":"user_basic_info"},
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
         |"not_id":{"cf":"rowkey", "col":"id", "type":"long"},
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
      .load().drop("not_id")

    val behavior_record_df:DataFrame = spark.read
      .option(HBaseTableCatalog.tableCatalog, behavior_record_write)
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .load()

    //user_basic_info_df.show(false)
    //+---+----------------------+---------+--------------------------------+----------+-----------+-----+---------+---------+-------------------+-------------------+---------+------+--------------+---------+-----------+--------+---------------+-------------+
    //|id |e_mail                |user_name|password                        |birthday  |mobile     |money|money_pwd|nick_name|last_login_time    |register_time      |qq       |job   |political_face|age_class|nationality|marriage|is_in_blacklist|constellation|
    //+---+----------------------+---------+--------------------------------+----------+-----------+-----+---------+---------+-------------------+-------------------+---------+------+--------------+---------+-----------+--------+---------------+-------------+
    //|1  |wcfr817e@yeah.net     |督咏     |7f930f90eb6604e837db06908cc95149|1992-05-31|13306834911|0.00 |         |         |2015-05-08 10:51:41|2008-08-06 11:48:12|null     |军人  |群众          |90后     |中国大陆   |已婚    |null           |双子座       |
    //|2  |xuwcbev9y@ask.com     |上磊     |7f930f90eb6604e837db06908cc95149|1983-10-11|15302753472|0.00 |         |         |2014-07-28 23:43:04|2008-08-10 05:37:32|未知     |军人  |无党派人士    |80后     |中国大陆   |已婚    |null           |天秤座       |
    //goods_catalog_df.show(false)
    //+---------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+------------+-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+----------------+------------------------+
    //|member_id|scanned_goods                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           |product_type|good_bought                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              |brand_preference|product_name            |
    //+---------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+------------+-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+----------------+------------------------+
    //|1        |[海尔滚筒洗衣机XQG70-1012 家家爱, 海尔冰箱BCD-206SM, 统帅投影仪众筹isee mini 1S, BCD-215SFES, 海尔冰箱BCD-196TMPI, 海尔冰箱BCD-231WDBB, XQB50-M1268 关爱, 海尔冰箱BC-93TMPF, TS40经典版, 海尔波轮洗衣机XQB50-M1268 关爱, MOOKA彩电U55H7, BC-50ES, 海尔吸尘器ZWBJ1000-2105B, 海尔电热水器EC8003-I, 海尔吸油烟机 CXW-180-JS721[CXW-180-JS721/JZT-QE3B(12T)], 海尔滚筒洗衣机XQG60-S1086AM, XQBM20-10EW, 海尔波轮洗衣机XPB85-287S 关爱, 海尔吸油烟机 CXW-200-JH901[CXW-200-JH901/JZT-QE3B(12T)套机], MS70-BZ1528, 统帅彩电TS48, XQB50-M1258关爱, 海尔烤箱ODT25-AU1, 海尔彩电底座 ZPB-TS66[U50H+ZPB-TS66（底座）底座套餐], XPM30-2008, 海尔电热水器EC6003-G, 海尔冰箱BCD-228S2, BCD-539WT(惠民), BCD-225SCZM, MOOKA彩电U50H7, 海尔冰箱健康卫士JZW-A01U1, 海尔燃气灶 JZT-QE3B(12T)[CXW-200-JH901/JZT-QE3B(12T)套机], BCD-196TMPI, 海尔波轮洗衣机XPB70-227HS 关爱, KFR-35GW/05KBP22A(DS)套机, 海尔电视D48MF7000, 海尔冰箱BC-50TMPR, 海尔行车记录仪 SIRIUS 7, 海尔滚筒洗衣机XQG70-B12866, XQG70-B12866 电商, MOOKA彩电48A5, ZW1200-201 虚网, 海尔冰箱BCD-118TMPA, 32EU3000, 海尔mini洗衣机iwash-1w, 海尔冰箱BCD-216SDN, 海尔电磁炉众筹D50MF5000, 海尔电熨斗HSR-1102L, 海尔吸油烟机CXW-200-E900C2, XQG60-1000J, XQG70-1012 家家爱, XQG70-1000J, 海尔彩电众筹D50MF5000, XQB50-728E, 海尔彩电55DU6000, XQB60-M1038, TS40M, 统帅彩电D50MF5000, 海尔冰箱BCD-206STPA, 海尔冰箱BCD-539WT(惠民), JZT-QE3B(12T), BCD-216SDN, 海尔冰箱BCD-225SFM, EC6002-R, XPB70-227HS 关爱, EC6002-D, XQG60-BS1086AM, 海尔波轮洗衣机XQS60-Z9288 至爱, D50MF5000, 海尔彩电底座 ZPB-TS66[mooka50寸全套餐], 海尔冰箱BCD-225SCZM, 海尔电热水器ES40H-Q1(ZE), 海尔滚筒洗衣机XQG70-1000J, XQB70-M1268 关爱, 海尔空气净化器 空气净化器KJ-F200/CA(白), BC/BD-202HT, BCD-118TMPA, 海尔电热水器小厨宝ES6.6U(W), XQG70-B12866, XPB85-287S 关爱, 海尔燃气灶 JZT-QE3G(12T)[CXW-200-C150/JZT-QE3G(12T)套机], XQG60-812 家家爱, KFR-35GW/05GJC23A-DS套机, 海尔电热水器ES60H-Q1(ZE), XQS60-Z9288 至爱, 海尔波轮洗衣机XQB55-M1268 关爱, BCD-231WDBB, 统帅冰箱BCD-206LST, 海尔燃气热水器JSQ24-UT(12T), 海尔吸油烟机CXW-180-JS721, 海尔波轮洗衣机XQB70-M1268 关爱, 海尔电磁炉C21-H2106 专供, 海尔彩电32DA3300, D48MF7000, BCD-206SM, 海尔除螨仪ZB403F, 海尔滚筒洗衣机XQG60-B10288, BCD-206LST, XQS70-Z9288 至爱, 海尔滚筒洗衣机XQG60-812 家家爱, BCD-225SLDA, TS48, MOOKA彩电55A5, 海尔电风扇FSJ4018E, BCD-539WL, 海尔电热水器EC6002-R, 海尔吸尘器ZW1409C, BC/BD-102HT, 海尔酒柜JC-46, KFR-35GW/05SEC21AT(茉莉白)套机, JSQ24-UT(12T), 海尔电热水器ES50H-Z4(ZE), BCD-133ES, 海尔mini洗衣机XQBM20-10EW, XQB60-728E, BCD-182LTMPA, BCD-206STPA, 海尔冰箱BCD-225SLDA, 海尔电热水器ES60H-M5(NT), 海尔冷柜BC/BD-102HT, 海尔原汁机众筹HYZ-101A, XPB65-1186BS AM, 海尔冰箱BCD-133ES, 海尔波轮洗衣机XQB60-728E]                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      |其他        |[海尔冰箱BC-93TMPF, 海尔波轮洗衣机XQB50-M1268 关爱, 海尔吸油烟机 CXW-180-JS721[CXW-180-JS721/JZT-QE3B(12T)], BCD-539WT(惠民), 海尔冰箱BCD-196TMPI, 海尔原汁机众筹HYZ-101A, 海尔吸油烟机CXW-180-JS721]                                                                                                                                                                                                                                                                                                                                                                    |海尔            |海尔冰箱BCD-196TMPI     |
    //|10       |[海尔空调KFR-35GW/05GJC23A-DS套机, BCD-216SDN, 海尔冰箱BCD-225SFM, EC6002-R, 海尔彩电 32DA3300, 海尔波轮洗衣机XQB60-M1268 关爱, XPB70-227HS 关爱, 海尔滚筒洗衣机XQG70-1012 家家爱, EC6002-D, 统帅投影仪众筹isee mini 1S, 海尔波轮洗衣机XQS60-Z9288 至爱, 海尔冰箱BCD-231WDBB, XQB50-M1268 关爱, 海尔冰箱BCD-225SCZM, 海尔滚筒洗衣机XQG70-1000J, XQB70-M1268 关爱, BCD-118TMPA, 海尔滚筒洗衣机 XQG70-1012 家家爱[BCD-225SEVF-ES+XQG70-1012 家家爱], XQBM20-10EW, ES50H-Q1(ZE), 海尔波轮洗衣机XPB85-287S 关爱, ES60H-Q1(ZE), 统帅彩电TS48, 海尔电热水器EC5002-Q6, 海尔空调KFR-35GW/05KBP22A(DS)套机, CXW-200-C150, XPB85-287S 关爱, XQB50-M1258关爱, XQG60-812 家家爱, KFR-32GW/01GJC13-DS套机, KFR-35GW/05GJC23A-DS套机, D50LW7100（阿里云）, 统帅彩电TS40M, 海尔彩电ZPB-TS68-1(LU55H7300), BCD-539WT(惠民), BCD-225SCZM, EC5002-Q6, 海尔电热水器FCD-H50H(E), 海尔波轮洗衣机XQB55-M1268 关爱, BCD-305WLV, 统帅冰箱BCD-206LST, 小厨宝ES6.6U(W), 海尔电暖器HK1701E, 海尔波轮洗衣机XQB70-M1268 关爱, BCD-196TMPI, D48MF7000, 海尔波轮洗衣机XQS70-Z9288 至爱, EC5002-R, 海尔吸油烟机CXW-219-JT901A, 55DU6000, BCD-206LST, 32EU3000, 海尔冰箱BCD-216SDN, U50H7, MOOKA彩电55A5, 统帅彩电众筹D50MF5000, 海尔电热水器EC6002-R, 海尔燃气热水器JSQ20-H(12T), MOOKA彩电 U50H7[mooka50寸全套餐], XQG60-1000J, BCD-225SFM, 海尔冰箱BC-50ES, XQG70-1012 家家爱, XQG70-1000J, 海尔彩电众筹D50MF5000, BCD-133ES, XQB60-M1038, 海尔电热水器EC5002-D, TS40M, BCD-225SEVF-ES, XQB60-728E, BCD-206STPA, 海尔冰箱BCD-206STPA, EC5002-D, ES40H-Q1(ZE), 海尔原汁机众筹HYZ-101A]                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 |其他        |[BCD-216SDN, EC6002-R, 海尔波轮洗衣机XQB60-M1268 关爱, XQB60-728E, 海尔彩电ZPB-TS68-1(LU55H7300), BCD-196TMPI]                                                                                                                                                                                                                                                                                                                                                                                                                                                           |其他            |BCD-216SDN              |

   // orders_info_catalog_df.show(false)
   // behavior_record_df.show(false)
   var res1 = user_basic_info_df.join(goods_catalog_df, user_basic_info_df.col("id").cast("string") === goods_catalog_df("member_id"))
     .drop(goods_catalog_df("member_id"))
//    res1.show(false)
    var res2=orders_info_catalog_df.join(behavior_record_df,behavior_record_df.col("global_user_id")===orders_info_catalog_df.col("member_id"))
      .drop(orders_info_catalog_df.col("member_id"))
//    res2.show(false)

    var res3=res1.join(res2,res1.col("id")===res2.col("global_user_id"))
      .drop(res2.col("global_user_id"))
    res3.show(false)

    def user_final_write =
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

    res3.write
      .option(HBaseTableCatalog.tableCatalog, user_final_write)
      .option(HBaseTableCatalog.newTable, "5")
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .save()
  }
}
