package utils

import com.google.gson.Gson
import EventUtils.Event

object ReportUtils {

  case class Report(
                     id: Int,
                     events: List[Event]
                     //words: String
                   )

  def parseFromJson(lines: Iterator[String]): Iterator[Report] = {
    val gson = new Gson
    lines.map(line => gson.fromJson(line, classOf[Report]))
  }
/*
  def printReport(report: Report): Unit = {
    println(report.id)
    println(report.first_name)
    println(report.last_name)
    println(report.latitude)
    println(report.longitude)
  }*/
}
