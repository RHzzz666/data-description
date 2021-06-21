import scala.collection.mutable.ListBuffer

object BASE {
  def main(args: Array[String]): Unit = {
    var a=0;
    val constnum=20
    var changable1="可以变化"
    val unchangable="不行"
    var message=s"var${changable1},但是val${unchangable}"
    println(message)
    var longtext=
      """
      |这段文字有点长
      |能换行
      """.stripMargin
    println(longtext)
    println(unchangable==changable1)
    println(unchangable.eq(changable1))
    if(a==1)println("a=1")else println("a!=1")
    var finalans={
      println("计算途中还能输出")

      println("git 测试")

      2+3
    }
    println(finalans)
    val nums=1.to(10)
    for(i <- nums){
      print(i)
    }
    println()
    for(i<-nums;j<-nums){
      print(i)
      if(j==10) println()
    }
    for(i<-nums if(i%2==0))print("1-10中的偶数有"+i)
    var v=for(i<-nums) yield i*10
    println(v)
    var i=0
    while(i<=10){
      println(i)
      i=i+1
    }
    max(12,2)
    var bigger= BASE max (12,2)
    println(bigger)
    var o=Array(1,2,3)
    println(o(2))
    for(i<-o)println(i)
    var yuanzu=(0,"zzt",22,"cd")
    println(yuanzu._1)
    val list1=List(1,2,3,4)
    println(list1(0))
    val list2 = ListBuffer(1, 2, 3)
    println(list2(0))
    list2+=1
    println(list2)
    list2 ++= List(5, 6, 7)
    println(list2)
    list2-=10
    println(list2.toList)
    println(list2.toArray)
    println(list2.isEmpty)
    val list3=List(12,34,56)
    val list4=list1 ++ list3
    list1.reverse
    var c = Set[Int]()
    val map = Map("zzt"->30, "lisi"->40)
    println(map("zzt"))
    for((x,y) <- map) println(s"$x $y")

  }
  def max(a:Int,b:Int)={
    if(a>b) a
    else b
  }
}
