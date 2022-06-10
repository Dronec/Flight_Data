package udfs

object udfs {

  val removeAdjacent = (myArray: Array[String]) => {
    val myList = myArray.toList
    val result = myList.head :: myList.sliding(2).collect { case Seq(a, b) if a != b => b }.toList
    result.toArray
  }
}
