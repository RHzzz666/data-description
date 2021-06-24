package individual_profile

import org.apache.spark
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, SparkSession}

case class People(name: String, age: Int)

object test_00 {
  def main(args: Array[String]): Unit = {
    val session: SparkSession = new spark.sql.SparkSession.Builder()
      .appName("HelloSpark")
      .master("local")
      .getOrCreate()

    import session.implicits._

    val peopleRDD: RDD[People] = session.sparkContext.parallelize(Seq(People("zhangsan", 10), People("lisi", 20)))

    val peopleDS: Dataset[People] = peopleRDD.toDS()

    val teenagers: Dataset[String] = peopleDS.where("age >= 10")
      .select("name")
      .as[String]

    teenagers.show()
  }
}
