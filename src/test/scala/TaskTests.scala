import org.scalatest.funsuite.AnyFunSuite
import Tasks._
import models._

import java.sql.Date

class TaskTests extends AnyFunSuite {

  val st = new Storage()

  import st.spark.implicits._

  val flightData = Seq(
    flight(179, 1, "co", "ir", Date.valueOf("2017-01-01")),
    flight(455, 4, "tk", "pk", Date.valueOf("2017-01-01")),
    flight(1226, 14, "cg", "tj", Date.valueOf("2017-01-03")),
    flight(1226, 19, "tj", "tj", Date.valueOf("2017-01-05")),
    flight(2359, 29, "co", "ir", Date.valueOf("2017-01-09")),
    flight(179, 32, "ir", "sg", Date.valueOf("2017-01-10")),
    flight(2677, 35, "au", "tj", Date.valueOf("2017-01-10")),
    flight(3290, 42, "ch", "th", Date.valueOf("2017-01-11")),
    flight(3346, 46, "se", "be", Date.valueOf("2017-01-13")),
    flight(2677, 50, "tj", "dk", Date.valueOf("2017-01-13")),
    flight(3704, 61, "ar", "bm", Date.valueOf("2017-01-19")),
    flight(3704, 86, "bm", "ch", Date.valueOf("2017-01-27")),
    flight(7376, 864, "th", "be", Date.valueOf("2017-11-18")),
    flight(9217, 868, "au", "be", Date.valueOf("2017-11-19")),
    flight(13264, 872, "ir", "ca", Date.valueOf("2017-11-20")),
    flight(1226, 880, "ir", "sg", Date.valueOf("2017-11-21")),
    flight(1226, 884, "sg", "se", Date.valueOf("2017-11-23")),
    flight(13264, 886, "ca", "sg", Date.valueOf("2017-11-23")),
    flight(1226, 888, "se", "dk", Date.valueOf("2017-11-24")),
    flight(7376, 893, "be", "th", Date.valueOf("2017-11-26")),
    flight(9763, 918, "fr", "ar", Date.valueOf("2017-12-06")),
    flight(14751, 923, "ch", "jo", Date.valueOf("2017-12-07")),
    flight(9217, 927, "be", "co", Date.valueOf("2017-12-09")),
    flight(15015, 933, "be", "cg", Date.valueOf("2017-12-10")),
    flight(14751, 939, "jo", "tj", Date.valueOf("2017-12-12")),
    flight(13264, 941, "sg", "be", Date.valueOf("2017-12-13")),
    flight(14751, 942, "tj", "us", Date.valueOf("2017-12-13")),
    flight(3346, 97, "be", "th", Date.valueOf("2017-02-01")),
    flight(455, 98, "pk", "ca", Date.valueOf("2017-02-01")),
    flight(3346, 106, "th", "sg", Date.valueOf("2017-02-06")),
    flight(1226, 115, "tj", "uk", Date.valueOf("2017-02-10")),
    flight(5183, 120, "us", "ch", Date.valueOf("2017-02-12")),
    flight(5183, 132, "ch", "th", Date.valueOf("2017-02-16")),
    flight(455, 136, "ca", "ir", Date.valueOf("2017-02-17")),
    flight(5183, 142, "th", "th", Date.valueOf("2017-02-19")),
    flight(179, 149, "sg", "se", Date.valueOf("2017-02-23")),
    flight(3346, 149, "sg", "se", Date.valueOf("2017-02-23")),
    flight(1226, 152, "uk", "se", Date.valueOf("2017-02-23")),
    flight(5872, 156, "il", "th", Date.valueOf("2017-02-25")),
    flight(5872, 171, "th", "tk", Date.valueOf("2017-03-01")),
    flight(5183, 172, "th", "no", Date.valueOf("2017-03-03")),
    flight(3290, 172, "th", "no", Date.valueOf("2017-03-03")),
    flight(455, 182, "ir", "jo", Date.valueOf("2017-03-07")),
    flight(2677, 187, "dk", "th", Date.valueOf("2017-03-09")),
    flight(13264, 947, "be", "cg", Date.valueOf("2017-12-14")),
    flight(14751, 951, "us", "ir", Date.valueOf("2017-12-16")),
    flight(15015, 955, "cg", "ir", Date.valueOf("2017-12-17")),
    flight(5872, 192, "tk", "fr", Date.valueOf("2017-03-10")),
    flight(455, 195, "jo", "cl", Date.valueOf("2017-03-11")),
    flight(6870, 206, "tj", "be", Date.valueOf("2017-03-15")),
    flight(2677, 211, "th", "th", Date.valueOf("2017-03-17")),
    flight(7376, 214, "au", "tj", Date.valueOf("2017-03-17")),
    flight(3290, 257, "no", "tj", Date.valueOf("2017-04-05")),
    flight(5183, 265, "no", "at", Date.valueOf("2017-04-07")),
    flight(5872, 280, "fr", "at", Date.valueOf("2017-04-12")),
    flight(6870, 285, "be", "co", Date.valueOf("2017-04-13")),
    flight(5183, 286, "at", "ca", Date.valueOf("2017-04-14")),
    flight(9763, 974, "ar", "pk", Date.valueOf("2017-12-23")),
    flight(5183, 975, "se", "bm", Date.valueOf("2017-12-23")),
    flight(9217, 999, "co", "cg", Date.valueOf("2017-12-31")),
    flight(5872, 286, "at", "ca", Date.valueOf("2017-04-14")),
    flight(6870, 300, "co", "be", Date.valueOf("2017-04-19")),
    flight(455, 320, "cl", "be", Date.valueOf("2017-04-25")),
    flight(6870, 322, "be", "ar", Date.valueOf("2017-04-25")),
    flight(3290, 323, "tj", "th", Date.valueOf("2017-04-26")),
    flight(9217, 345, "co", "dk", Date.valueOf("2017-05-01")),
    flight(455, 352, "be", "cn", Date.valueOf("2017-05-03")),
    flight(6870, 354, "ar", "at", Date.valueOf("2017-05-03")),
    flight(3346, 356, "se", "us", Date.valueOf("2017-05-05")),
    flight(5183, 360, "ca", "th", Date.valueOf("2017-05-07")),
    flight(455, 362, "cn", "tk", Date.valueOf("2017-05-08")),
    flight(6870, 363, "at", "iq", Date.valueOf("2017-05-09")),
    flight(9763, 365, "ar", "cg", Date.valueOf("2017-05-10")),
    flight(6870, 373, "iq", "tk", Date.valueOf("2017-05-12")),
    flight(3346, 377, "us", "ar", Date.valueOf("2017-05-13")),
    flight(9217, 378, "dk", "ar", Date.valueOf("2017-05-13")),
    flight(3290, 392, "th", "no", Date.valueOf("2017-05-17")),
    flight(5183, 392, "th", "no", Date.valueOf("2017-05-17")),
    flight(9763, 396, "cg", "dk", Date.valueOf("2017-05-18")),
    flight(5183, 397, "no", "se", Date.valueOf("2017-05-18")),
    flight(9763, 404, "dk", "sg", Date.valueOf("2017-05-20")),
    flight(5872, 405, "ca", "fr", Date.valueOf("2017-05-21")),
    flight(3704, 417, "ch", "th", Date.valueOf("2017-05-24")),
    flight(10323, 421, "pk", "uk", Date.valueOf("2017-05-26")),
    flight(3346, 425, "ar", "ch", Date.valueOf("2017-05-27")),
    flight(9763, 426, "sg", "bm", Date.valueOf("2017-05-27")),
    flight(6870, 428, "tk", "fr", Date.valueOf("2017-05-29")),
    flight(9217, 454, "ar", "jo", Date.valueOf("2017-06-10")),
    flight(11414, 493, "pk", "cg", Date.valueOf("2017-06-25")),
    flight(5872, 495, "fr", "cn", Date.valueOf("2017-06-25")),
    flight(455, 496, "tk", "cn", Date.valueOf("2017-06-26")),
    flight(9217, 505, "jo", "jo", Date.valueOf("2017-06-30")),
    flight(6870, 510, "fr", "tk", Date.valueOf("2017-07-01")),
    flight(3346, 556, "ch", "cg", Date.valueOf("2017-07-17")),
    flight(3290, 561, "no", "cn", Date.valueOf("2017-07-19")),
    flight(10323, 597, "uk", "il", Date.valueOf("2017-08-01")),
    flight(10323, 599, "il", "tk", Date.valueOf("2017-08-02")),
    flight(455, 602, "cn", "sg", Date.valueOf("2017-08-03")),
    flight(3290, 602, "cn", "sg", Date.valueOf("2017-08-03")),
    flight(3346, 605, "cg", "uk", Date.valueOf("2017-08-04")),
    flight(1226, 621, "se", "ir", Date.valueOf("2017-08-11")),
    flight(9763, 626, "bm", "fr", Date.valueOf("2017-08-14")),
    flight(3290, 629, "sg", "jo", Date.valueOf("2017-08-14")),
    flight(7376, 630, "tj", "cn", Date.valueOf("2017-08-15")),
    flight(7376, 635, "cn", "bm", Date.valueOf("2017-08-16")),
    flight(3290, 645, "jo", "ir", Date.valueOf("2017-08-19")),
    flight(5872, 647, "cn", "uk", Date.valueOf("2017-08-20")),
    flight(455, 672, "sg", "sg", Date.valueOf("2017-09-01")),
    flight(10323, 673, "tk", "cl", Date.valueOf("2017-09-01")),
    flight(455, 695, "sg", "ca", Date.valueOf("2017-09-10")),
    flight(3290, 700, "ir", "bm", Date.valueOf("2017-09-12")),
    flight(13006, 701, "cn", "il", Date.valueOf("2017-09-12")),
    flight(10323, 716, "cl", "cg", Date.valueOf("2017-09-18")),
    flight(3290, 729, "bm", "uk", Date.valueOf("2017-09-21")),
    flight(7376, 729, "bm", "uk", Date.valueOf("2017-09-21")),
    flight(13264, 739, "co", "us", Date.valueOf("2017-09-24")),
    flight(9217, 747, "jo", "ch", Date.valueOf("2017-09-27")),
    flight(455, 750, "ca", "au", Date.valueOf("2017-09-28")),
    flight(6870, 754, "tk", "ch", Date.valueOf("2017-09-30")),
    flight(7376, 765, "uk", "nl", Date.valueOf("2017-10-04")),
    flight(455, 766, "au", "au", Date.valueOf("2017-10-06")),
    flight(3290, 779, "uk", "cn", Date.valueOf("2017-10-11")),
    flight(9217, 818, "ch", "fr", Date.valueOf("2017-10-25")),
    flight(9217, 821, "fr", "fr", Date.valueOf("2017-10-28")),
    flight(6870, 836, "ch", "il", Date.valueOf("2017-11-06")),
    flight(9217, 843, "fr", "au", Date.valueOf("2017-11-08")),
    flight(3290, 844, "cn", "sg", Date.valueOf("2017-11-08")),
    flight(3290, 846, "sg", "il", Date.valueOf("2017-11-09")),
    flight(7376, 847, "nl", "th", Date.valueOf("2017-11-10")),
    flight(13264, 848, "us", "ir", Date.valueOf("2017-11-10"))
  ).toDS()

  val passengers = Seq(
    passenger(14751, "Napoleon", "Gaylene"),
    passenger(2359, "Katherin", "Shanell"),
    passenger(5872, "Stevie", "Steven"),
    passenger(3346, "Margarita", "Gerri"),
    passenger(3704, "Earle", "Candis"),
    passenger(1226, "Trent", "Omer"),
    passenger(2677, "Janee", "Lillia"),
    passenger(179, "Gita", "Chastity"),
    passenger(9763, "Hilton", "Jaquelyn"),
    passenger(11414, "Leo", "Margaret"),
    passenger(6870, "Tama", "Bok"),
    passenger(3290, "Logan", "Anya"),
    passenger(13264, "Lowell", "Kathryne"),
    passenger(455, "Maritza", "Maxima"),
    passenger(13006, "Yuri", "Joyce"),
    passenger(10323, "Latasha", "Estell"),
    passenger(7376, "Kaycee", "Kiersten"),
    passenger(15015, "Curtis", "Abraham"),
    passenger(9217, "Verena", "Josefine"),
    passenger(5183, "Loan", "Latonya")
  ).toDS()

  val myTasks = new Tasks(flightData, passengers)
  test("Task1") {
    val expected = Seq(
      (1, 12),
      (2, 11),
      (3, 9),
      (4, 9),
      (5, 21),
      (6, 5),
      (7, 3),
      (8, 11),
      (9, 11),
      (10, 5),
      (11, 14),
      (12, 13)
    ).toDF("Month", "Number of Flights")
    val actual = myTasks.task1.orderBy("Month")
    assert(actual.collect() === expected.collect())
  }

  test("Task2") {
    val expected = Seq(
      (179, 3, "Gita", "Chastity"),
      (455, 14, "Maritza", "Maxima"),
      (1226, 8, "Trent", "Omer"),
      (2359, 1, "Katherin", "Shanell"),
      (2677, 4, "Janee", "Lillia"),
      (3290, 14, "Logan", "Anya"),
      (3346, 9, "Margarita", "Gerri"),
      (3704, 3, "Earle", "Candis"),
      (5183, 10, "Loan", "Latonya"),
      (5872, 8, "Stevie", "Steven"),
      (6870, 11, "Tama", "Bok"),
      (7376, 8, "Kaycee", "Kiersten"),
      (9217, 11, "Verena", "Josefine"),
      (9763, 7, "Hilton", "Jaquelyn"),
      (10323, 5, "Latasha", "Estell"),
      (11414, 1, "Leo", "Margaret"),
      (13006, 1, "Yuri", "Joyce"),
      (13264, 6, "Lowell", "Kathryne"),
      (14751, 4, "Napoleon", "Gaylene"),
      (15015, 2, "Curtis", "Abraham")
    ).toDF("Passenger ID", "Number of Flights", "First name", "Last name")
    val actual = myTasks.task2.orderBy("passengerId")
    assert(actual.collect() === expected.collect())
  }

  test("Task3") {
    val expected = Seq(
      (179, 4),
      (455, 13),
      (1226, 5),
      (2359, 2),
      (2677, 4),
      (3290, 11),
      (3346, 9),
      (3704, 4),
      (5183, 10),
      (5872, 8),
      (6870, 12),
      (7376, 4),
      (9217, 10),
      (9763, 8),
      (10323, 4),
      (11414, 2),
      (13006, 2),
      (13264, 7),
      (14751, 5),
      (15015, 3)
    ).toDF("Passenger ID", "Longest Run")
    val actual = myTasks.task3
    assert(actual.collect() === expected.collect())
  }

  test("Task4") {
    val expected = Seq(
      (3290, 5183, 2)
    ).toDF("Passenger 1 ID", "Passenger 2 ID", "Number of flights together")
    val actual = myTasks.task4(2)
    println(actual.show())
    assert(actual.collect() === expected.collect())
  }
}
