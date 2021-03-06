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
         |"frequency_range(???,???,???)":{"cf":"cf", "col":"frequency_range(???,???,???)", "type":"string"},
         |"exchange_item_rate":{"cf":"cf", "col":"exchange_item_rate", "type":"string"},
         |"return_item_rate":{"cf":"cf", "col":"return_item_rate", "type":"string"},
         |"exchange_item_rate(???,???,???)":{"cf":"cf", "col":"exchange_item_rate(???,???,???)", "type":"string"},
         |"return_item_rate(???,???,???)":{"cf":"cf", "col":"return_item_rate(???,???,???)", "type":"string"},
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
    //|1  |wcfr817e@yeah.net     |??????     |7f930f90eb6604e837db06908cc95149|1992-05-31|13306834911|0.00 |         |         |2015-05-08 10:51:41|2008-08-06 11:48:12|null     |??????  |??????          |90???     |????????????   |??????    |null           |?????????       |
    //|2  |xuwcbev9y@ask.com     |??????     |7f930f90eb6604e837db06908cc95149|1983-10-11|15302753472|0.00 |         |         |2014-07-28 23:43:04|2008-08-10 05:37:32|??????     |??????  |???????????????    |80???     |????????????   |??????    |null           |?????????       |
    //goods_catalog_df.show(false)
    //+---------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+------------+-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+----------------+------------------------+
    //|member_id|scanned_goods                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           |product_type|good_bought                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              |brand_preference|product_name            |
    //+---------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+------------+-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+----------------+------------------------+
    //|1        |[?????????????????????XQG70-1012 ?????????, ????????????BCD-206SM, ?????????????????????isee mini 1S, BCD-215SFES, ????????????BCD-196TMPI, ????????????BCD-231WDBB, XQB50-M1268 ??????, ????????????BC-93TMPF, TS40?????????, ?????????????????????XQB50-M1268 ??????, MOOKA??????U55H7, BC-50ES, ???????????????ZWBJ1000-2105B, ??????????????????EC8003-I, ?????????????????? CXW-180-JS721[CXW-180-JS721/JZT-QE3B(12T)], ?????????????????????XQG60-S1086AM, XQBM20-10EW, ?????????????????????XPB85-287S ??????, ?????????????????? CXW-200-JH901[CXW-200-JH901/JZT-QE3B(12T)??????], MS70-BZ1528, ????????????TS48, XQB50-M1258??????, ????????????ODT25-AU1, ?????????????????? ZPB-TS66[U50H+ZPB-TS66????????????????????????], XPM30-2008, ??????????????????EC6003-G, ????????????BCD-228S2, BCD-539WT(??????), BCD-225SCZM, MOOKA??????U50H7, ????????????????????????JZW-A01U1, ??????????????? JZT-QE3B(12T)[CXW-200-JH901/JZT-QE3B(12T)??????], BCD-196TMPI, ?????????????????????XPB70-227HS ??????, KFR-35GW/05KBP22A(DS)??????, ????????????D48MF7000, ????????????BC-50TMPR, ????????????????????? SIRIUS 7, ?????????????????????XQG70-B12866, XQG70-B12866 ??????, MOOKA??????48A5, ZW1200-201 ??????, ????????????BCD-118TMPA, 32EU3000, ??????mini?????????iwash-1w, ????????????BCD-216SDN, ?????????????????????D50MF5000, ???????????????HSR-1102L, ??????????????????CXW-200-E900C2, XQG60-1000J, XQG70-1012 ?????????, XQG70-1000J, ??????????????????D50MF5000, XQB50-728E, ????????????55DU6000, XQB60-M1038, TS40M, ????????????D50MF5000, ????????????BCD-206STPA, ????????????BCD-539WT(??????), JZT-QE3B(12T), BCD-216SDN, ????????????BCD-225SFM, EC6002-R, XPB70-227HS ??????, EC6002-D, XQG60-BS1086AM, ?????????????????????XQS60-Z9288 ??????, D50MF5000, ?????????????????? ZPB-TS66[mooka50????????????], ????????????BCD-225SCZM, ??????????????????ES40H-Q1(ZE), ?????????????????????XQG70-1000J, XQB70-M1268 ??????, ????????????????????? ???????????????KJ-F200/CA(???), BC/BD-202HT, BCD-118TMPA, ???????????????????????????ES6.6U(W), XQG70-B12866, XPB85-287S ??????, ??????????????? JZT-QE3G(12T)[CXW-200-C150/JZT-QE3G(12T)??????], XQG60-812 ?????????, KFR-35GW/05GJC23A-DS??????, ??????????????????ES60H-Q1(ZE), XQS60-Z9288 ??????, ?????????????????????XQB55-M1268 ??????, BCD-231WDBB, ????????????BCD-206LST, ?????????????????????JSQ24-UT(12T), ??????????????????CXW-180-JS721, ?????????????????????XQB70-M1268 ??????, ???????????????C21-H2106 ??????, ????????????32DA3300, D48MF7000, BCD-206SM, ???????????????ZB403F, ?????????????????????XQG60-B10288, BCD-206LST, XQS70-Z9288 ??????, ?????????????????????XQG60-812 ?????????, BCD-225SLDA, TS48, MOOKA??????55A5, ???????????????FSJ4018E, BCD-539WL, ??????????????????EC6002-R, ???????????????ZW1409C, BC/BD-102HT, ????????????JC-46, KFR-35GW/05SEC21AT(?????????)??????, JSQ24-UT(12T), ??????????????????ES50H-Z4(ZE), BCD-133ES, ??????mini?????????XQBM20-10EW, XQB60-728E, BCD-182LTMPA, BCD-206STPA, ????????????BCD-225SLDA, ??????????????????ES60H-M5(NT), ????????????BC/BD-102HT, ?????????????????????HYZ-101A, XPB65-1186BS AM, ????????????BCD-133ES, ?????????????????????XQB60-728E]                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      |??????        |[????????????BC-93TMPF, ?????????????????????XQB50-M1268 ??????, ?????????????????? CXW-180-JS721[CXW-180-JS721/JZT-QE3B(12T)], BCD-539WT(??????), ????????????BCD-196TMPI, ?????????????????????HYZ-101A, ??????????????????CXW-180-JS721]                                                                                                                                                                                                                                                                                                                                                                    |??????            |????????????BCD-196TMPI     |
    //|10       |[????????????KFR-35GW/05GJC23A-DS??????, BCD-216SDN, ????????????BCD-225SFM, EC6002-R, ???????????? 32DA3300, ?????????????????????XQB60-M1268 ??????, XPB70-227HS ??????, ?????????????????????XQG70-1012 ?????????, EC6002-D, ?????????????????????isee mini 1S, ?????????????????????XQS60-Z9288 ??????, ????????????BCD-231WDBB, XQB50-M1268 ??????, ????????????BCD-225SCZM, ?????????????????????XQG70-1000J, XQB70-M1268 ??????, BCD-118TMPA, ????????????????????? XQG70-1012 ?????????[BCD-225SEVF-ES+XQG70-1012 ?????????], XQBM20-10EW, ES50H-Q1(ZE), ?????????????????????XPB85-287S ??????, ES60H-Q1(ZE), ????????????TS48, ??????????????????EC5002-Q6, ????????????KFR-35GW/05KBP22A(DS)??????, CXW-200-C150, XPB85-287S ??????, XQB50-M1258??????, XQG60-812 ?????????, KFR-32GW/01GJC13-DS??????, KFR-35GW/05GJC23A-DS??????, D50LW7100???????????????, ????????????TS40M, ????????????ZPB-TS68-1(LU55H7300), BCD-539WT(??????), BCD-225SCZM, EC5002-Q6, ??????????????????FCD-H50H(E), ?????????????????????XQB55-M1268 ??????, BCD-305WLV, ????????????BCD-206LST, ?????????ES6.6U(W), ???????????????HK1701E, ?????????????????????XQB70-M1268 ??????, BCD-196TMPI, D48MF7000, ?????????????????????XQS70-Z9288 ??????, EC5002-R, ??????????????????CXW-219-JT901A, 55DU6000, BCD-206LST, 32EU3000, ????????????BCD-216SDN, U50H7, MOOKA??????55A5, ??????????????????D50MF5000, ??????????????????EC6002-R, ?????????????????????JSQ20-H(12T), MOOKA?????? U50H7[mooka50????????????], XQG60-1000J, BCD-225SFM, ????????????BC-50ES, XQG70-1012 ?????????, XQG70-1000J, ??????????????????D50MF5000, BCD-133ES, XQB60-M1038, ??????????????????EC5002-D, TS40M, BCD-225SEVF-ES, XQB60-728E, BCD-206STPA, ????????????BCD-206STPA, EC5002-D, ES40H-Q1(ZE), ?????????????????????HYZ-101A]                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 |??????        |[BCD-216SDN, EC6002-R, ?????????????????????XQB60-M1268 ??????, XQB60-728E, ????????????ZPB-TS68-1(LU55H7300), BCD-196TMPI]                                                                                                                                                                                                                                                                                                                                                                                                                                                           |??????            |BCD-216SDN              |

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
         |"frequency_range(???,???,???)":{"cf":"cf", "col":"frequency_range(???,???,???)", "type":"string"},
         |"exchange_item_rate":{"cf":"cf", "col":"exchange_item_rate", "type":"string"},
         |"return_item_rate":{"cf":"cf", "col":"return_item_rate", "type":"string"},
         |"exchange_item_rate(???,???,???)":{"cf":"cf", "col":"exchange_item_rate(???,???,???)", "type":"string"},
         |"return_item_rate(???,???,???)":{"cf":"cf", "col":"return_item_rate(???,???,???)", "type":"string"},
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
