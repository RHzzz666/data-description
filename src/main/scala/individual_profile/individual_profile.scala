package individual_profile

import org.apache.spark.sql.execution.datasources.hbase.HBaseTableCatalog
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

object individual_profile {
  def main(args: Array[String]): Unit = {
    def catalog =
      s"""{
         |"table":{"namespace":"default", "name":"tbl_users"},
         |"rowkey":"id",
         |"columns":{
         |"id":{"cf":"rowkey", "col":"id", "type":"string"},
         |"username":{"cf":"cf", "col":"username", "type":"string"},
         |"gender":{"cf":"cf", "col":"gender", "type":"string"},
         |"email":{"cf":"cf", "col":"email", "type":"string"},
         |"mobile":{"cf":"cf", "col":"mobile", "type":"string"},
         |"job":{"cf":"cf", "col":"job", "type":"string"},
         |"politicalFace":{"cf":"cf", "col":"politicalFace", "type":"string"},
         |"nationality":{"cf":"cf", "col":"nationality", "type":"string"},
         |"marriage":{"cf":"cf", "col":"marriage", "type":"string"},
         |"birthday":{"cf":"cf", "col":"birthday", "type":"string"},
         |"is_blackList":{"cf":"cf", "col":"is_blackList", "type":"string"}
         |}
         |}""".stripMargin

    //"education":{"cf":"cf", "col":"education", "type":"string"}
    val spark = SparkSession.builder()
      .appName("shc test")
      .master("local[10]")
      .getOrCreate()

    import spark.implicits._

    val readDF: DataFrame = spark.read
      .option(HBaseTableCatalog.tableCatalog, catalog)
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .load()

    val result = readDF.withColumn("birthday", to_timestamp('birthday))
      .select('id, 'birthday.cast("string"), 'username, 'email, 'mobile,
        when('gender === "1", "男")
          .when('gender === "2", "女")
          .otherwise("未知")
          .as("gender"),
        when('job === "1", "学生")
          .when('job === "2", "公务员")
          .when('job === "3", "军人")
          .when('job === "4", "警察")
          .when('job === "5", "教师")
          .when('job === "6", "白领")
          .otherwise("其他")
          .as("job"),
        when('politicalFace === "1", "群众")
          .when('politicalFace === "2", "党员")
          .when('politicalFace === "3", "无党派人士")
          .otherwise("无")
          .as("politicalFace"),
        when('nationality === "1", "中国大陆")
          .when('nationality === "2", "中国香港")
          .when('nationality === "3", "中国澳门")
          .when('nationality === "4", "中国台湾")
          .when('nationality === "5", "其他")
          .otherwise("其他")
          .as("nationality"),
        when('marriage === "1", "未婚")
          .when('marriage === "2", "已婚")
          .when('marriage === "3", "离异")
          .otherwise("其他")
          .as("marriage"),
        when(year('birthday) between(1950, 1959), "50后")
          .when(year('birthday) between(1960, 1969), "60后")
          .when(year('birthday) between(1970, 1979), "70后")
          .when(year('birthday) between(1980, 1989), "80后")
          .when(year('birthday) between(1990, 1999), "90后")
          .when(year('birthday) between(2000, 2009), "00后")
          .when(year('birthday) between(2010, 2019), "10后")
          .when(year('birthday) between(2020, 2029), "20后")
          .otherwise("其他")
          .as("age"),
        when(month('birthday) === "1" and (dayofmonth('birthday) between(1, 20)), "摩羯座")
          .when(month('birthday) === "1" and (dayofmonth('birthday) between(20, 31)), "水瓶座")
          .when(month('birthday) === "2" and (dayofmonth('birthday) between(1, 19)), "水瓶座")
          .when(month('birthday) === "2" and (dayofmonth('birthday) between(20, 29)), "双鱼座")
          .when(month('birthday) === "03" and (dayofmonth('birthday) between(21, 31)), "白羊座")
          .when(month('birthday) === "03" and (dayofmonth('birthday) between(1, 20)), "双鱼座")
          .when(month('birthday) === "04" and (dayofmonth('birthday) between(1, 20)), "白羊座")
          .when(month('birthday) === "04" and (dayofmonth('birthday) between(21, 30)), "金牛座")
          .when(month('birthday) === "05" and (dayofmonth('birthday) between(1, 21)), "金牛座")
          .when(month('birthday) === "05" and (dayofmonth('birthday) between(22, 31)), "双子座")
          .when(month('birthday) === "06" and (dayofmonth('birthday) between(1, 21)), "双子座")
          .when(month('birthday) === "06" and (dayofmonth('birthday) between(22, 30)), "巨蟹座")
          .when(month('birthday) === "07" and (dayofmonth('birthday) between(1, 22)), "巨蟹座")
          .when(month('birthday) === "07" and (dayofmonth('birthday) between(23, 31)), "狮子座")
          .when(month('birthday) === "08" and (dayofmonth('birthday) between(1, 22)), "狮子座")
          .when(month('birthday) === "08" and (dayofmonth('birthday) between(23, 31)), "处女座")
          .when(month('birthday) === "09" and (dayofmonth('birthday) between(1, 23)), "处女座")
          .when(month('birthday) === "09" and (dayofmonth('birthday) between(24, 30)), "天秤座")
          .when(month('birthday) === "10" and (dayofmonth('birthday) between(1, 23)), "天秤座")
          .when(month('birthday) === "10" and (dayofmonth('birthday) between(24, 30)), "天蝎座")
          .when(month('birthday) === "11" and (dayofmonth('birthday) between(1, 22)), "天蝎座")
          .when(month('birthday) === "11" and (dayofmonth('birthday) between(23, 30)), "射手座")
          .when(month('birthday) === "12" and (dayofmonth('birthday) between(1, 21)), "射手座")
          .when(month('birthday) === "12" and (dayofmonth('birthday) between(22, 31)), "摩羯座")
          .otherwise("其他")
          .as("Constellation"),
        when('is_blackList === "false", "非黑名单")
          .when('is_blackList === "ture", "黑名单")
          .otherwise("其他")
          .as("is_blackList")
        //          when('education === "1", "小学")
        //          .when('education === "2", "初中")
        //          .when('education === "3", "高中")
        //          .when('education === "4", "大专")
        //          .when('education === "5", "本科")
        //          .when('education === "6", "研究生")
        //          .when('education === "7", "博士")
        //          .otherwise("其他")
        //          .as("education")
      )
    result.printSchema()
    result.show()

    println("开始写入user的信息")
    def catalogWrite =
      s"""{
         |"table":{"namespace":"default", "name":"user"},
         |"rowkey":"id",
         |"columns":{
         |"id":{"cf":"rowkey", "col":"id", "type":"string"},
         |"username":{"cf":"cf", "col":"username", "type":"string"},
         |"gender":{"cf":"cf", "col":"gender", "type":"string"},
         |"email":{"cf":"cf", "col":"email", "type":"string"},
         |"mobile":{"cf":"cf", "col":"mobile", "type":"string"},
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
    //"education":{"cf":"cf", "col":"education", "type":"string"}
    result.write
      .option(HBaseTableCatalog.tableCatalog, catalogWrite)
      .option(HBaseTableCatalog.newTable, "5")
      .format("org.apache.spark.sql.execution.adatasources.hbase")
      .save()
  }
}
