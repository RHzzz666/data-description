package individual_profile

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object test0624 {
  case class People(name:String,age:Int)
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    val spark = SparkSession.builder()
      .appName("shc test")
      .master("local[10]")
      .getOrCreate()

    var rdd = spark.sparkContext.makeRDD(Seq(People("zhangsan",12),People("lisi",12)))
    import spark.implicits._
    val df =rdd.toDF()
    df.printSchema()
    df.show()
    spark.stop()
  }
}
