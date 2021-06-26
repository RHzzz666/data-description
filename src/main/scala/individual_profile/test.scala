package individual_profile

object test {
  def main(args: Array[String]): Unit = {
    def string_last_3_char(str:String):String = {
      val len = str.length
      val sub_str = if(len > 3) str.substring(len-3,len) else str
      if(sub_str(0)=='0' && sub_str(1)=='0') sub_str.substring(2)
      else if(sub_str(0)=='0') sub_str.substring(1,3)
      else sub_str
    }
    println(string_last_3_char("12093"))
    println(string_last_3_char("12000"))
    println(string_last_3_char("12010"))
    println(string_last_3_char("12110"))
  }
}
