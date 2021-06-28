import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.execution.datasources.hbase.HBaseTableCatalog
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import scala.util.parsing.json._
import java.text.SimpleDateFormat
import java.util.Date
import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.sql.{Column, DataFrame, SparkSession, functions}
import org.apache.spark.sql.execution.datasources.hbase.HBaseTableCatalog
import org.apache.spark.sql.functions._
object DataAnalysis {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("shc test")
      .master("local[10]")
      .getOrCreate()

    import spark.implicits._

    //读取消费周期的格式
    def formatRead =
      s"""{
         |  "table":{"namespace":"default", "name":"tbl_orders"},
         |  "rowkey":"id",
         |  "columns":{
         |    "id":{"cf":"rowkey", "col":"id", "type":"long"},
         |    "memberId":{"cf":"cf", "col":"memberId", "type":"string"},
         |    "orderStatus":{"cf":"cf", "col":"orderStatus", "type":"string"},
         |    "finishTime":{"cf":"cf", "col":"finishTime", "type":"string"},
         |    "paidAmount":{"cf":"cf", "col":"paidAmount", "type":"string"}
         |  }
         |}""".stripMargin

    //读取数据
    val source=spark.read
      .option(HBaseTableCatalog.tableCatalog, formatRead)
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .load()


    val validSource=source.where('orderStatus==="202")
      .withColumn("Id",substring('memberId,-3,3))
    //处理数据并回显购物周期
    //利用finishTime来计算购物周期
    val shopRoute=validSource.withColumn("time",unix_timestamp(from_unixtime('finishTime)))
      .groupBy('Id)
      .agg(count('Id)as "shopTime",from_unixtime(min('time)) as "firstTime",from_unixtime(max('time)) as "lastTime")
      .withColumn("shopRoute",datediff(('lastTime),('firstTime))+1)
      .withColumn("frequency",'shopRoute/'shopTime)
      .drop('shopTime)
      .drop('firstTime)
      .drop('lastTime)
      .drop('shopRoute)
      //.show(false)
    //找最近一次消费
    val lastShop=validSource.withColumn("time",unix_timestamp(from_unixtime('finishTime)))
      .groupBy('Id)
      .agg(from_unixtime(max('time)) as "lastTime")
      .withColumn("now",current_date())
      .withColumn("untilNow",datediff(('now),('lastTime))+1)
      .drop('now)
      .drop('lastTime)
    //.show(false)
    //统计平均每单的金额
    //处理数据，计算平均每单价格
    val averageAmount=validSource.groupBy('Id)
      .agg(count('Id)as "shopTime",sum('paidAmount) as "sumAmount")
      .withColumn("averageAmount",'sumAmount/'shopTime)
      .drop('shopTime)
      .drop('sumAmount)
    //.show(false)

    //链接以上数据

    val subfeatureTable=averageAmount.join(lastShop,averageAmount.col("Id")===lastShop.col("Id"))
      .drop(averageAmount.col("Id"))
        //.show(false)
    val featureTable=subfeatureTable.join(shopRoute,subfeatureTable.col("Id")===shopRoute.col("Id"))
        .drop(subfeatureTable.col("Id"))
//        .show(false)

    def featureWrite=
      s"""{
         |"table":{"namespace":"default", "name":"featureTable"},
         |"rowkey":"Id",
         |"columns":{
         |"Id":{"cf":"rowkey", "col":"Id", "type":"string"},
         |"averageAmount":{"cf":"cf", "col":"averageAmount", "type":"string"},
         |"untilNow":{"cf":"cf", "col":"untilNow", "type":"string"},
         |"frequency":{"cf":"cf", "col":"frequency", "type":"string"}
         |}
         |}""".stripMargin

    featureTable.write
      .option(HBaseTableCatalog.tableCatalog, featureWrite)
      .option(HBaseTableCatalog.newTable, "5")
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .save()

    //kmeans方法训练



    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)

    def predictAmount =
      s"""{
         |  "table":{"namespace":"default", "name":"tbl_orders"},
         |  "rowkey":"id",
         |  "columns":{
         |    "id":{"cf":"rowkey", "col":"id", "type":"string"},
         |    "memberId":{"cf":"cf", "col":"memberId", "type":"string"},
         |    "orderSn":{"cf":"cf", "col":"orderSn", "type":"string"},
         |    "orderAmount":{"cf":"cf", "col":"orderAmount", "type":"string"},
         |    "finishTime":{"cf":"cf", "col":"finishTime", "type":"string"}
         |  }
         |}""".stripMargin

    val predictAmountSource: DataFrame = spark.read
      .option(HBaseTableCatalog.tableCatalog, predictAmount)
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .load()
    val rencency = "rencency"
    val frequency = "frequency"
    val sumAmount = "sumAmount"
    val featureAmount = "featureAmount"
    val predictValue = "predict"
    val days_range = 660

    // 统计距离最近一次消费的时间
    val recencyCol = datediff(date_sub(current_timestamp(), days_range), from_unixtime(max('finishTime))) as rencency
    // 统计订单总数
    val frequencyCol = count('orderSn) as frequency
    // 统计订单总金额
    val moneyTotalCol = sum('orderAmount) as sumAmount

    val RFMResult = predictAmountSource.withColumn("Id",substring('memberId,-3,3).cast("long"))
      .groupBy('Id)
      .agg(recencyCol, frequencyCol, moneyTotalCol)
    RFMResult.show(false)
    //2.为RFM打分
    //R: 1-3天=5分，4-6天=4分，7-9天=3分，10-15天=2分，大于16天=1分
    //F: ≥200=5分，150-199=4分，100-149=3分，50-99=2分，1-49=1分
    //M: ≥20w=5分，10-19w=4分，5-9w=3分，1-4w=2分，<1w=1分
    val recencyScore: Column = functions.when((col(rencency) >= 1) && (col(rencency) <= 3), 5)
      .when((col(rencency) >= 4) && (col(rencency) <= 6), 4)
      .when((col(rencency) >= 7) && (col(rencency) <= 9), 3)
      .when((col(rencency) >= 10) && (col(rencency) <= 15), 2)
      .when(col(rencency) >= 16, 1)
      .as(rencency)

    val frequencyScore: Column = functions.when(col(frequency) >= 200, 5)
      .when((col(frequency) >= 150) && (col(frequency) <= 199), 4)
      .when((col(frequency) >= 100) && (col(frequency) <= 149), 3)
      .when((col(frequency) >= 50) && (col(frequency) <= 99), 2)
      .when((col(frequency) >= 1) && (col(frequency) <= 49), 1)
      .as(frequency)

    val moneyTotalScore: Column = functions.when(col(sumAmount) >= 200000, 5)
      .when(col(sumAmount).between(100000, 199999), 4)
      .when(col(sumAmount).between(50000, 99999), 3)
      .when(col(sumAmount).between(10000, 49999), 2)
      .when(col(sumAmount) <= 9999, 1)
      .as(sumAmount)

    val RFMScoreResult = RFMResult.select('Id, recencyScore, frequencyScore, moneyTotalScore)

    val vectorDF = new VectorAssembler()
      .setInputCols(Array(rencency, frequency, sumAmount))
      .setOutputCol(featureAmount)
      .transform(RFMScoreResult)

    val kmeans = new KMeans()
      .setK(7)
      .setSeed(100)
      .setMaxIter(2)
      .setFeaturesCol(featureAmount)
      .setPredictionCol(predictValue)

    // train model
    val model = kmeans.fit(vectorDF)

    val predicted = model.transform(vectorDF)

    predicted.orderBy('predict.desc)
      .show(false)
      //.drop('featureAmount)

    //将预测信息和用户连接起来
    def formatReadUser=
      s"""{
         |  "table":{"namespace":"default", "name":"tbl_users"},
         |  "rowkey":"id",
         |  "columns":{
         |    "id":{"cf":"rowkey", "col":"id", "type":"long"},
         |    "username":{"cf":"cf", "col":"username", "type":"string"}
         |  }
         |}""".stripMargin

    val linkSource: DataFrame=spark.read
      .option(HBaseTableCatalog.tableCatalog, formatReadUser)
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .load()

//    linkSource.show(false)

    val userClassTable=linkSource.join(predicted,
      linkSource.col("id")===predicted.col("Id"))
      .drop(predicted.col("Id"))
      .drop('rencency)
      .drop('frequency)
      .drop('sumAmount)
      .drop('featureAmount)
      .withColumnRenamed("predict","class")
    val userClassTableResult = userClassTable.select('id,'username,
      when('class === "0", "很低")
        .when('class === "1", "低")
        .when('class === "2", "中下")
        .when('class === "3", "中")
        .when('class === "4", "中上")
        .when('class === "5", "高")
        .when('class === "6", "很高")
        .otherwise("其他")
        .as("consumptionAblity")
    )
      //.show(false)

    def consumptionAblityWrite=
      s"""{
         |  "table":{"namespace":"default", "name":"user_class"},
         |  "rowkey":"id",
         |  "columns":{
         |    "id":{"cf":"rowkey", "col":"id", "type":"long"},
         |    "username":{"cf":"cf", "col":"username", "type":"string"},
         |    "consumptionAblity":{"cf":"cf", "col":"consumptionAblity", "type":"string"}
         |  }
         |}""".stripMargin
    userClassTableResult.write
      .option(HBaseTableCatalog.tableCatalog, consumptionAblityWrite)
      .option(HBaseTableCatalog.newTable, "5")
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .save()

    //是否是优惠券用户

    def predictDiscountInOrder=
      s"""{
         |  "table":{"namespace":"default", "name":"tbl_orders"},
         |  "rowkey":"id",
         |  "columns":{
         |    "id":{"cf":"rowkey", "col":"id", "type":"long"},
         |    "memberId":{"cf":"cf", "col":"memberId", "type":"string"},
         |    "orderSn":{"cf":"cf", "col":"orderSn", "type":"string"},
         |    "finishTime":{"cf":"cf", "col":"finishTime", "type":"string"},
         |    "isGiftCardOrder":{"cf":"cf", "col":"isGiftCardOrder", "type":"string"},
         |    "couponCodeValue":{"cf":"cf", "col":"couponCodeValue", "type":"string"},
         |    "balanceAmount":{"cf":"cf", "col":"balanceAmount", "type":"string"}
         |  }
         |}""".stripMargin
    val predictDiscountSourceInOrder: DataFrame = spark.read
      .option(HBaseTableCatalog.tableCatalog, predictDiscountInOrder)
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .load()

    def predictDiscount=
      s"""{
         |  "table":{"namespace":"default", "name":"tbl_goods"},
         |  "rowkey":"id",
         |  "columns":{
         |    "id":{"cf":"rowkey", "col":"id", "type":"long"},
         |    "balanceAmount":{"cf":"cf", "col":"balanceAmount", "type":"string"},
         |    "productAmount":{"cf":"cf", "col":"productAmount", "type":"string"}
         |  }
         |}""".stripMargin
    //读取数据
    val predictDiscountSource: DataFrame = spark.read
      .option(HBaseTableCatalog.tableCatalog, predictDiscount)
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .load()
      .withColumn("Id",substring('id,-3,3).cast("long"))
    //预处理第一次统计每个人有多少笔订单
    val discountAll=predictDiscountSource.groupBy('Id)
      .agg(count('Id) as "all")
    //预处理第二次计算每笔订单是原价的多少
    val pre1= predictDiscountSource
      .withColumn("discount",'productAmount/('productAmount+'balanceAmount)*100)
      .withColumn("discount",'discount.cast("double"))
    //预处理第三次统计每个人高折扣的订单数
    val pre2=pre1.groupBy('Id)
      .agg(
        sum(when('discount<=50,1).otherwise(0)) as "VeryHighAddict",
        sum(when('discount>=50 and 'discount<60,1).otherwise(0)) as "HighAddict",
        sum(when('discount>=60 and 'discount<70,1).otherwise(0)) as "MiddleHighAddict",
        sum(when('discount>=70 and 'discount<80,1).otherwise(0)) as "MiddleAddict",
        sum(when('discount>=80 and 'discount<90,1).otherwise(0)) as "MiddleLowAddict",
        sum(when('discount>=90 and 'discount<=100,1).otherwise(0)) as "LowAddict")
      //.show(false)
    //预处理第4次，计算每个人每种折扣比例的总数
    val pre3=pre2.join(discountAll,discountAll.col("Id")===pre2.col("Id"))
      .drop(pre2.col("Id"))
      .withColumn("VeryHighRate",'VeryHighAddict/'all)
      .withColumn("HighRate",'HighAddict/'all)
      .withColumn("MiddleHighRate",'MiddleHighAddict/'all)
      .withColumn("MiddleRate",'MiddleAddict/'all)
      .withColumn("MiddleLowRate",'MiddleLowAddict/'all)
      .withColumn("LowRate",'LowAddict/'all)
      .drop('HighAddict)
      .drop('MiddleHighAddict)
      .drop('MiddleAddict)
      .drop('MiddleLowAddict)
      .drop('VeryHighAddict)
      .drop('LowAddict)
      .drop('all)
    pre3.where('Id==="103")
      .show()
    //pre3是最终的有关特征的表

    val veryHigh = "veryHigh"
    val high = "high"
    val middleHigh = "middleHigh"
    val middle = "middle"
    val low = "low"
    val featureAmount2 = "featureAmount"
    val predictValue2 = "predict"

    val countVeryHigh='VeryHighRate as veryHigh
    val countHigh=pre3.col("HighRate") as high
    val countMiddleHigh=pre3.col("MiddleHighRate") as middleHigh
    val countMiddleAddict=pre3.col("MiddleRate") as middle
    val countLowAddict=pre3.col("LowRate") as low


    val resultDISCOUNT=pre3.groupBy('Id,'VeryHighRate,'HighRate,'MiddleHighRate,'MiddleRate,'LowRate)
      .agg(countVeryHigh,countHigh,countMiddleHigh,countMiddleAddict,countLowAddict)
    //2.为RFM打分
    //R: 1-3天=5分，4-6天=4分，7-9天=3分，10-15天=2分，大于16天=1分
    //F: ≥200=5分，150-199=4分，100-149=3分，50-99=2分，1-49=1分
    //M: ≥20w=5分，10-19w=4分，5-9w=3分，1-4w=2分，<1w=1分
    val veryHighScore: Column = functions.when((col(veryHigh) >= 0.004), 5)
      .when((col(veryHigh) >= 0.003) && (col(veryHigh) <0.004), 4)
      .when((col(veryHigh) >= 0.002) && (col(veryHigh) <0.003), 3)
      .when((col(veryHigh) >= 0.001) && (col(veryHigh) <0.002), 2)
      .when(col(veryHigh) >= 0 && (col(veryHigh) <0.001),1)
      .as(veryHigh)

    val highScore: Column = functions.when((col(high) >= 0.008), 8)
      .when((col(high) >= 0.007) && (col(high) <0.008), 7)
      .when((col(high) >= 0.006) && (col(high) <0.007), 6)
      .when((col(high) >= 0.005) && (col(high) <0.006), 5)
      .when((col(high) >= 0.004) && (col(high) <0.006), 4)
      .when((col(high) >= 0.003) && (col(high) <0.005), 3)
      .when((col(high) >= 0.002) && (col(high) <0.003), 2)
      .when((col(high) >= 0.001) && (col(high) <0.002), 1)
      .when((col(high) >= 0) && (col(high) <0.001), 0)
      .as(high)

    val middleHighScore: Column = functions.when((col(middleHigh) >= 0.008), 8)
      .when((col(middleHigh) >= 0.007) && (col(middleHigh) <0.008), 7)
      .when((col(middleHigh) >= 0.006) && (col(middleHigh) <0.007), 6)
      .when((col(middleHigh) >= 0.005) && (col(middleHigh) <0.006), 5)
      .when((col(middleHigh) >= 0.004) && (col(middleHigh) <0.006), 4)
      .when((col(middleHigh) >= 0.003) && (col(middleHigh) <0.005), 3)
      .when((col(middleHigh) >= 0.002) && (col(middleHigh) <0.003), 2)
      .when((col(middleHigh) >= 0.001) && (col(middleHigh) <0.002), 1)
      .when((col(middleHigh) >= 0) && (col(middleHigh) <0.001), 0)
      .as(middleHigh)

    val middleScore: Column = functions.when((col(middle) >= 0.003), 4)
      .when((col(middle) >= 0.002) && (col(middle) <0.003), 3)
      .when((col(middle) >= 0.001) && (col(middle) <0.002), 2)
      .when((col(middle) >= 0) && (col(middle) <0.001), 1)
      .as(middle)

    val lowScore: Column = functions.when((col(low) >= 1), 5)
      .when((col(low) >= 0.99) && (col(low) <1), 4)
      .when((col(low) >= 0.98) && (col(low) <0.99), 3)
      .when((col(low) >= 0.97) && (col(low) <0.98), 2)
      .when(col(low) <0.97,1)
      .as(low)


    val RFMScoreResult2 = resultDISCOUNT.select('Id, veryHighScore, highScore, middleHighScore,middleScore,lowScore)

    val vectorDF2 = new VectorAssembler()
      .setInputCols(Array(veryHigh, high, middleHigh,middle,low))
      .setOutputCol(featureAmount)
      .transform(RFMScoreResult2)

    val kmeans2 = new KMeans()
      .setK(5)
      .setSeed(100)
      .setMaxIter(2)
      .setFeaturesCol(featureAmount2)
      .setPredictionCol(predictValue2)

    // train model
    val model2 = kmeans2.fit(vectorDF2)

    val predicted2 = model2.transform(vectorDF2)
        .orderBy('predict)

    predicted2.show(false)
    //0-4数字越高越不是优惠券用户
    //predicted2表中是表示用户

    val resultDiscount=predicted2.drop('veryHigh)
      .drop('high)
      .drop('middleHigh)
      .drop('middle)
      .drop('low)
      .drop('featureAmount)
    def discountWrite=
      s"""{
         |  "table":{"namespace":"default", "name":"user_discount"},
         |  "rowkey":"Id",
         |  "columns":{
         |    "Id":{"cf":"rowkey", "col":"Id", "type":"long"},
         |    "predict":{"cf":"cf", "col":"predict", "type":"long"}
         |  }
         |}""".stripMargin

    resultDiscount.write
      .option(HBaseTableCatalog.tableCatalog, discountWrite)
      .option(HBaseTableCatalog.newTable, "5")
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .save()

    //统计每个用户每周的消费记录

    def weekRead=
      s"""{
         |  "table":{"namespace":"default", "name":"tbl_orders"},
         |  "rowkey":"id",
         |  "columns":{
         |    "id":{"cf":"rowkey", "col":"id", "type":"long"},
         |    "memberId":{"cf":"cf", "col":"memberId", "type":"string"},
         |    "finishTime":{"cf":"cf", "col":"finishTime", "type":"string"}
         |  }
         |}""".stripMargin

    val weekSource=spark.read
      .option(HBaseTableCatalog.tableCatalog, weekRead)
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .load()
      .withColumn("Id",substring('id,-3,3).cast("long"))

    val weekProcess=weekSource.withColumn("finishWeek", weekofyear(from_unixtime('finishTime)))
      .withColumn("year",date_format(from_unixtime('finishTime),"yyyy"))
        .groupBy('Id,'finishWeek,'year)
        .agg(count('memberId)as 'weeklyTime)
        .orderBy('Id)
        .withColumn("rowKey",monotonically_increasing_id())
    def weekInfoWrite=
      s"""{
         |  "table":{"namespace":"default", "name":"user_weekly"},
         |  "rowkey":"rowKey",
         |  "columns":{
         |    "Id":{"cf":"cf", "col":"Id", "type":"long"},
         |    "rowKey":{"cf":"rowkey", "col":"rowKey", "type":"long"},
         |    "finishWeek":{"cf":"cf", "col":"finishWeek", "type":"string"},
         |    "weeklyTime":{"cf":"cf", "col":"weeklyTime", "type":"string"},
         |    "year":{"cf":"cf", "col":"year", "type":"string"}
         |  }
         |}""".stripMargin
    weekProcess.write
      .option(HBaseTableCatalog.tableCatalog, weekInfoWrite)
      .option(HBaseTableCatalog.newTable, "5")
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .save()

    //群体画像
    //统计消费能力不同的人的比例

    def peopleCount=
      s"""{
         |  "table":{"namespace":"default", "name":"user_class_1"},
         |  "rowkey":"id",
         |  "columns":{
         |    "id":{"cf":"rowkey", "col":"id", "type":"string"},
         |    "consumptionAblity":{"cf":"cf", "col":"consumptionAblity", "type":"string"}
         |  }
         |}""".stripMargin

    val peopleSource1=spark.read
      .option(HBaseTableCatalog.tableCatalog, peopleCount)
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .load()

    peopleSource1.show(false)

    val resultPeople=peopleSource1.groupBy('consumptionAblity)
      .agg(count('id)as "sumOfPeople")
    resultPeople.show(false)
    def peopleInfoWrite=
      s"""{
         |  "table":{"namespace":"default", "name":"people_class"},
         |  "rowkey":"consumptionAblity",
         |  "columns":{
         |    "consumptionAblity":{"cf":"rowkey", "col":"consumptionAblity", "type":"string"},
         |    "sumOfPeople":{"cf":"cf", "col":"sumOfPeople", "type":"string"}
         |  }
         |}""".stripMargin
    resultPeople.write
      .option(HBaseTableCatalog.tableCatalog, peopleInfoWrite)
      .option(HBaseTableCatalog.newTable, "5")
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .save()

    def peopleInfoRead=
      s"""{
         |  "table":{"namespace":"default", "name":"people_class"},
         |  "rowkey":"consumptionAblity",
         |  "columns":{
         |    "consumptionAblity":{"cf":"rowkey", "col":"consumptionAblity", "type":"string"},
         |    "sumOfPeople":{"cf":"cf", "col":"sumOfPeople", "type":"long"}
         |  }
         |}""".stripMargin

    val peopleSource=spark.read
      .option(HBaseTableCatalog.tableCatalog, peopleInfoRead)
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .load()

    peopleSource.show()

    def peopleDiscountRead=
      s"""{
         |  "table":{"namespace":"default", "name":"user_discount"},
         |  "rowkey":"Id",
         |  "columns":{
         |    "Id":{"cf":"rowkey", "col":"Id", "type":"long"},
         |    "predict":{"cf":"cf", "col":"predict", "type":"int"}
         |  }
         |}""".stripMargin

    val peopleDiscountSource=spark.read
      .option(HBaseTableCatalog.tableCatalog, peopleDiscountRead)
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .load()
    peopleDiscountSource.show(false)

    val userClass=peopleDiscountSource.select('predict,'Id,
      when('predict===0,"最低")
        .when('predict===1,"低")
        .when('predict===2,"中")
        .when('predict===3,"高")
        .when('predict===4,"最高")
        .otherwise("其他")
        .as("消费优惠券依赖度"))

    userClass.show(false)
    def personClassWrite=
      s"""{
         |  "table":{"namespace":"default", "name":"user_discount_withChinese"},
         |  "rowkey":"Id",
         |  "columns":{
         |    "Id":{"cf":"rowkey", "col":"Id", "type":"long"},
         |    "消费优惠券依赖度":{"cf":"cf", "col":"消费优惠券依赖度", "type":"string"}
         |  }
         |}""".stripMargin
    userClass.write
      .option(HBaseTableCatalog.tableCatalog, personClassWrite)
      .option(HBaseTableCatalog.newTable, "5")
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .save()

//
//    val userClassTable=linkSource.join(predicted,
//      linkSource.col("id")===predicted.col("Id"))
//      .drop(predicted.col("Id"))
//      .drop('rencency)
//      .drop('frequency)
//      .drop('sumAmount)
//      .drop('featureAmount)
//      .withColumnRenamed("predict","class")
//    val userClassTableResult = userClassTable.select('id,'username,
//      when('class === "0", "很低")
//        .when('class === "1", "低")
//        .when('class === "2", "中下")
//        .when('class === "3", "中")
//        .when('class === "4", "中上")
//        .when('class === "5", "高")
//        .when('class === "6", "很高")
//        .otherwise("其他")
//        .as("consumptionAblity")
//    )
    val discountPeople=peopleDiscountSource.groupBy('predict)
      .agg(count('Id)as "sumPeople")
      .withColumnRenamed("predict","class")
    val peopleClass=discountPeople.select('class,'sumPeople,
          when('class===0,"很低")
        .when('class===1,"低")
        .when('class===2,"中")
        .when('class===3,"高")
        .when('class===4,"很高")
            .otherwise("其他")
      .as("消费优惠券依赖度"))

    peopleClass.show(false)
    def peopleClassWrite=
      s"""{
         |  "table":{"namespace":"default", "name":"people_discount"},
         |  "rowkey":"class",
         |  "columns":{
         |    "class":{"cf":"rowkey", "col":"class", "type":"string"},
         |    "消费优惠券依赖度":{"cf":"cf", "col":"消费优惠券依赖度", "type":"string"},
         |    "sumOfPeople":{"cf":"cf", "col":"sumOfPeople", "type":"string"}
         |  }
         |}""".stripMargin
    peopleClass.write
      .option(HBaseTableCatalog.tableCatalog, peopleClassWrite)
      .option(HBaseTableCatalog.newTable, "5")
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .save()
  }
}