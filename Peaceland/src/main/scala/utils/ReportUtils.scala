package utils

import com.google.gson.Gson

object ReportUtils {

  case class Report(
                     id: Int,
                     first_name: String,
                     last_name: String,
                     latitude: String,
                     longitude: String,
                     words: String
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
