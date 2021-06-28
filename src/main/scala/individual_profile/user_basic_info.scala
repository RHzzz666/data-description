package individual_profile

//记录用户基本信息
//
//+---+--------------------+---------+--------------------------------+----------+-----------+-----+---------+---------+-------------------+-------------------+--------+------+--------------+---------+-----------+--------+---------------+-------------+
//|id |e_mail              |user_name|password                        |birthday  |mobile     |money|money_pwd|nick_name|last_login_time    |register_time      |qq      |job   |political_face|age_class|nationality|marriage|is_in_blacklist|constellation|
//+---+--------------------+---------+--------------------------------+----------+-----------+-----+---------+---------+-------------------+-------------------+--------+------+--------------+---------+-----------+--------+---------------+-------------+
//|1  |wcfr817e@yeah.net   |督咏     |7f930f90eb6604e837db06908cc95149|1992-05-31|13306834911|0.00 |         |         |2015-05-08 10:51:41|2008-08-06 11:48:12|null    |军人  |群众          |90后     |中国大陆   |已婚    |非黑名单       |双子座       |
//|10 |mqqqmf@yahoo.com    |西香     |96802a851b4a7295fb09122b9aa79c18|1980-10-13|13108330898|0.00 |         |         |2013-12-26 15:55:08|2008-08-16 04:42:08|未知    |教师  |党员          |80后     |中国大陆   |已婚    |非黑名单       |天秤座       |
//|100|ksxk9zy6m@gmail.com |史艳红   |96802a851b4a7295fb09122b9aa79c18|1993-10-28|13702525668|0.00 |         |         |2014-01-10 10:10:59|2013-12-23 17:13:27|未知    |军人  |党员          |90后     |中国大陆   |未婚    |非黑名单       |天蝎座       |


import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.execution.datasources.hbase.HBaseTableCatalog
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

object user_basic_info {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    val spark = SparkSession.builder()
      .appName("User_basic_info")
      .master("local")
      .getOrCreate()

    //我所需要的数据表格
    def user_basic_info_catalog: String = {
      s"""{
         |"table":{"namespace":"default","name":"tbl_users"},
         |"rowkey":"id",
         |"columns":{
         |"id":{"cf":"rowkey","col":"id","type":"string"},
         |"e_mail":{"cf":"cf","col":"email","type":"string"},
         |"user_name":{"cf":"cf","col":"username","type":"string"},
         |"register_time":{"cf":"cf","col":"registerTime","type":"string"},
         |"password":{"cf":"cf","col":"password","type":"string"},
         |"last_login_time":{"cf":"cf","col":"lastLoginTime","type":"string"},
         |"qq":{"cf":"cf","col":"qq","type":"string"},
         |"job":{"cf":"cf","col":"job","type":"string"},
         |"mobile":{"cf":"cf","col":"mobile","type":"string"},
         |"political_face":{"cf":"cf","col":"politicalFace","type":"string"},
         |"birthday":{"cf":"cf","col":"birthday","type":"string"},
         |"nationality":{"cf":"cf","col":"nationality","type":"string"},
         |"marriage":{"cf":"cf","col":"marriage","type":"string"},
         |"money":{"cf":"cf","col":"money","type":"string"},
         |"money_pwd":{"cf":"cf","col":"moneyPwd","type":"string"},
         |"nick_name":{"cf":"cf","col":"nick_name","type":"string"},
         |"is_blackList":{"cf":"cf","col":"is_blackList","type":"string"},
         |"gender_for_drop":{"cf":"cf","col":"gender","type":"string"},
         |"zipcode":{"cf":"cf","col":"zipcode","type":"string"}
         |}
         |}
       """.stripMargin
    }

    //org.json4s.JsonAST$JString cannot be cast to org.json4s.JsonAST$JObject
    //这个错误一般对应的 catalog 内部的内容写错了

    import spark.implicits._

    val df: DataFrame = spark.read
      .option(HBaseTableCatalog.tableCatalog, user_basic_info_catalog)
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .load()

    //对dataframe的操作
    //对用户的注册时间进行显示

    val res = df
      .withColumn("current_time", current_timestamp())
      .withColumn("last_log", datediff('current_time, 'last_login_time))
      .select('id, 'e_mail, 'user_name, 'password, 'birthday,
        'mobile, 'money, 'money_pwd, 'nick_name, from_unixtime('last_login_time).as("last_login_time"),
        when('register_time === null, "未知")
          .otherwise(from_unixtime('register_time))
          .as("register_time"),
        //qq号码
        when('qq === "", "未知")
          .when('qq === "null", "未知")
          .otherwise('qq)
          .as("qq"),
        //工作
        when('job === "1", "学生")
          .when('job === "2", "公务员")
          .when('job === "3", "军人")
          .when('job === "4", "警察")
          .when('job === "5", "教师")
          .when('job === "6", "白领")
          .otherwise("其他工作")
          .as("job"),
        //政治面貌
        when('political_face === "1", "群众")
          .when('political_face === "2", "党员")
          .when('political_face === "3", "无党派人士")
          .otherwise("其他")
          .as("political_face"),
        //年龄段
        when(year(to_timestamp('birthday)) between(1950, 1959), "50后")
          .when(year(to_timestamp('birthday)) between(1960, 1969), "60后")
          .when(year(to_timestamp('birthday)) between(1970, 1979), "70后")
          .when(year(to_timestamp('birthday)) between(1980, 1989), "80后")
          .when(year(to_timestamp('birthday)) between(1990, 1999), "90后")
          .when(year(to_timestamp('birthday)) between(2000, 2009), "00后")
          .when(year(to_timestamp('birthday)) between(2010, 2019), "10后")
          .when(year(to_timestamp('birthday)) between(2020, 2029), "20后")
          .as('age_class),
        //国家:1中国大陆、2中国香港、3中国澳门、4中国台湾、5其他
        when('nationality === "1", "中国大陆")
          .when('nationality === "2", "中国香港")
          .when('nationality === "3", "中国澳门")
          .when('nationality === "4", "中国台湾")
          .when('nationality === "5", "其他")
          .otherwise("未知地区")
          .as("nationality"),
        when('marriage === "1", "未婚")
          .when('marriage === "2", "已婚")
          .when('marriage === "3", "离异")
          .otherwise("未知")
          .as("marriage"),
        //判断黑名单
        when('is_blackList === "true", "黑名单")
          .when('is_blackList === "false", value = "非黑名单")
          .as("is_in_blacklist"),

        when(month(to_timestamp('birthday)) === "1" and (dayofmonth(to_timestamp('birthday)) between(1, 20)), "摩羯座")
          .when(month(to_timestamp('birthday)) === "1" and (dayofmonth(to_timestamp('birthday)) between(20, 31)), "水瓶座")
          .when(month(to_timestamp('birthday)) === "2" and (dayofmonth(to_timestamp('birthday)) between(1, 19)), "水瓶座")
          .when(month(to_timestamp('birthday)) === "2" and (dayofmonth(to_timestamp('birthday)) between(20, 29)), "双鱼座")
          .when(month(to_timestamp('birthday)) === "03" and (dayofmonth(to_timestamp('birthday)) between(21, 31)), "白羊座")
          .when(month(to_timestamp('birthday)) === "03" and (dayofmonth(to_timestamp('birthday)) between(1, 20)), "双鱼座")
          .when(month(to_timestamp('birthday)) === "04" and (dayofmonth(to_timestamp('birthday)) between(1, 20)), "白羊座")
          .when(month(to_timestamp('birthday)) === "04" and (dayofmonth(to_timestamp('birthday)) between(21, 30)), "金牛座")
          .when(month(to_timestamp('birthday)) === "05" and (dayofmonth(to_timestamp('birthday)) between(1, 21)), "金牛座")
          .when(month(to_timestamp('birthday)) === "05" and (dayofmonth(to_timestamp('birthday)) between(22, 31)), "双子座")
          .when(month(to_timestamp('birthday)) === "06" and (dayofmonth(to_timestamp('birthday)) between(1, 21)), "双子座")
          .when(month(to_timestamp('birthday)) === "06" and (dayofmonth(to_timestamp('birthday)) between(22, 30)), "巨蟹座")
          .when(month(to_timestamp('birthday)) === "07" and (dayofmonth(to_timestamp('birthday)) between(1, 22)), "巨蟹座")
          .when(month(to_timestamp('birthday)) === "07" and (dayofmonth(to_timestamp('birthday)) between(23, 31)), "狮子座")
          .when(month(to_timestamp('birthday)) === "08" and (dayofmonth(to_timestamp('birthday)) between(1, 22)), "狮子座")
          .when(month(to_timestamp('birthday)) === "08" and (dayofmonth(to_timestamp('birthday)) between(23, 31)), "处女座")
          .when(month(to_timestamp('birthday)) === "09" and (dayofmonth(to_timestamp('birthday)) between(1, 23)), "处女座")
          .when(month(to_timestamp('birthday)) === "09" and (dayofmonth(to_timestamp('birthday)) between(24, 30)), "天秤座")
          .when(month(to_timestamp('birthday)) === "10" and (dayofmonth(to_timestamp('birthday)) between(1, 23)), "天秤座")
          .when(month(to_timestamp('birthday)) === "10" and (dayofmonth(to_timestamp('birthday)) between(24, 30)), "天蝎座")
          .when(month(to_timestamp('birthday)) === "11" and (dayofmonth(to_timestamp('birthday)) between(1, 22)), "天蝎座")
          .when(month(to_timestamp('birthday)) === "11" and (dayofmonth(to_timestamp('birthday)) between(23, 30)), "射手座")
          .when(month(to_timestamp('birthday)) === "12" and (dayofmonth(to_timestamp('birthday)) between(1, 21)), "射手座")
          .when(month(to_timestamp('birthday)) === "12" and (dayofmonth(to_timestamp('birthday)) between(22, 31)), "摩羯座")
          .otherwise("其他")
          .as("constellation"),
        when(col("zipcode") like "10%", "北京")
          .when(col("zipcode") like "20%", "上海")
          .when(col("zipcode") like "518%", "深圳")
          .when(col("zipcode") like "51%", "广州")
          .when(col("zipcode") like "31%", "杭州")
          .when(col("zipcode") like "215%", "苏州")
          .when(col("zipcode") like "1%", "东北地区")
          .when(col("zipcode") like "2%", "华东地区")
          .when(col("zipcode") like "3%", "东南地区")
          .when(col("zipcode") like "4%", "华中地区")
          .when(col("zipcode") like "5%", "华南地区")
          .when(col("zipcode") like "6%", "西南地区")
          .when(col("zipcode") like "7%", "西北地区")
          .when(col("zipcode") like "8%", "西部地区")
          .when(col("zipcode") like "9%", "港澳台地区")
          .otherwise("其它地区")
          .as("region")

        , when(col("gender_for_drop") === "1", "男")
          .when(col("gender_for_drop") === "2", "女")
          .otherwise("其它")
          .as("gender"),

        when('last_log <= 1, "1天以内")
          .when('last_log between(1, 7), "7天以内")
          .when('last_log between(7, 14), "14天以内")
          .when('last_log between(14, 30), "30天以内")
          .otherwise("30天未登录")
          .as("recent_log")
        //添加上次登陆时间

      ).drop(df.col("zipcode"))
      .drop(df.col("gender_for_drop"))
      .drop('current_time)


    println("开始展示schema和20行测试数据")
    res.printSchema()
    res.show(false)

    println("开始写入数据！！！")

    def user_basic_info_write_catalog =
      s"""{
         |"table":{"namespace":"default","name":"user_basic_info"},
         |"rowkey":"id",
         |"columns":{
         |"id":{"cf":"rowkey","col":"id","type":"string"},
         |"e_mail":{"cf":"cf","col":"e_mail","type":"string"},
         |"user_name":{"cf":"cf","col":"user_name","type":"string"},
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
         |"recent_log":{"cf":"cf","col":"recent_log","type":"string"}
         |}
         |}
           """.stripMargin

    res.write
      .option(HBaseTableCatalog.tableCatalog, user_basic_info_write_catalog)
      .option(HBaseTableCatalog.newTable, "5")
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .save()
  }
}

