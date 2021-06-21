object DAY2 {
  trait sayHello{
    def say(msg:String)
  }
  class Person(var name:String ="",var age:Int=0) extends sayHello {
    println("构造器声明的是name："+name+"age："+age)
    def getName()=name
    def setName(name: String)=this.name=name
    def getAge()=age
    def setAge(age:Int)=this.age=age
    def printHello(msg: String="Hello"): Unit = {
      println(msg)
    }

    override def say(msg: String): Unit =println("Hello,"+msg)
  }
  case class SCU(name :String,id:Int)
  class Student extends Person{
    override def getName(): String = "你好啊"+super.getName()
  }
  class Customer(var name:String = "", var address:String = "") {
    def this(arr:Array[String]) = {
      this(arr(0), arr(1))
    }
  }
  object Dog{
    val leg=4
  }
  object myPrint{
    def printSpliter() = {
      println("-" * 10)
    }
  }
  object Person {
    def apply(name:String, age:Int) = new Person(name, age)
  }
  def main(args: Array[String]): Unit = {
    val list1 = List(1, 2, 3, 4)
    list1.foreach(println(_))
    println(list1.map(_ + 1))
    val list2 = List("zhe ju hua you kong ge", "zhejuhuameiyoukongge")
    println(list2.flatMap(_.split(" ")))
    println(list1.filter(_ % 2 == 0))
    println(list1.reverse)
    println(list1.sortWith(_ < _))
    val newMap = List("diyigeren" -> "male", "diergeren" -> "female", "disangeren" -> "male")
    println(newMap.groupBy(_._2))
    println(list1.reduce(_ + _))
    println(list1.fold(0)(_ + _))
    var person = new Person()
    person.setAge(20)
    person.setName("张三")
    println(person.getAge())
    println(person.getName())
    var person2 = new Person("李四", 4)
    var customer = new Customer("张三", "成都")
    println(Dog.leg)
    myPrint.printSpliter()
    var firstman = Person("张三", 20)
    println(firstman.name)
    println(firstman.age)
    var student = new Student()
    student.setName("学生")
    person2.say("李四")
    var student2 = SCU("王五", 5)
    println(student2.toString)
    var student3 = student2.copy(name = "赵六")
    println(student3.toString)
    val func1: Int => String = (num: Int) => "*" * num
    println((1 to 10).map(func1))
    val func_num2star = (num: Int) => "*" * num
    println(list1.map(func_num2star))

    def add(x: Int, y: Int)(op: (Int, Int) => Int) = {
      op(x, y)
    }

    println(add(1, 2) {
      (x, y) => x + y
    })
    val y=10
    val minus=(x:Int)=>{
      x-y
    }
    println(minus(20))
  }
}
