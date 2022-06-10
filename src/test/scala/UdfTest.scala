import org.scalatest.funsuite.AnyFunSuite
import udfs.udfs.removeAdjacent

class UdfTest extends AnyFunSuite {
  test("RemoveAdjacentTest") {
    val testArray = Array("aa", "ab", "ab", "ac", "ad", "ad", "ae")
    val resultArray = Array("aa", "ab", "ac", "ad", "ae")
    assert(removeAdjacent(testArray) === resultArray)
  }
}
