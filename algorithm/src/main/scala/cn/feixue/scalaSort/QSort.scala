package cn.feixue.scalaSort

/**
  * scala版本的快排
  */
object QSort {

  def qSort(arr: List[Int]) : List[Int]= arr match {
    case Nil => Nil
    case ::(hear,tail) => qSort(tail.filter(_ <= hear)) ++ List(hear) ++ qSort(tail.filter( _ > hear))
  }

  def main(args: Array[String]): Unit = {

    val arr = List(1,5,3,6,4,4,5)

    val newArr=qSort(arr)

    println(newArr)



  }

}
