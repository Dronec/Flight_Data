package models

case class flight(
                   passengerId: Int,
                   flightId: Int,
                   from: String,
                   to: String,
                   date: java.sql.Date
                 ) extends genericModel
