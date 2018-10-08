package cn.feixue.scalaSort

/**
  * scala实现冒泡排序
  */
object MaoPao {

  def sort(list: List[Int]): List[Int] = list match {
    case List() => List()
    case head :: tail => compute(head, sort(tail))
  }

  def compute(data: Int, dataSet: List[Int]): List[Int] = dataSet match {
    case List() => List(data)
    case head :: tail => if (data <= head) {
      data :: dataSet
    } else {
      head :: compute(data, tail)
    }
  }

  def main(args: Array[String]) {
    val list = List(3, 12, 43, 23, 7, 1, 2, 0)
    println(sort(list))
  }

}
