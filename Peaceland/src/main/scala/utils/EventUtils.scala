package utils

import com.google.gson.Gson

object EventUtils {

  // Event Structure
  case class Event(
                    row: Int,
                    lat: Float,
                    long: Float,
                    first_name: String,
                    last_name: String,
                    score: Int
                  )

  // Get lines in the event structure from existing json file
  def parseFromJson(lines: Iterator[String]): Iterator[Event] = {
    val gson = new Gson
    lines.map(line => gson.fromJson(line, classOf[Event]))
  }

  // write event in json string format
  def writeEvent(event : Event): String = {
    val gson = new Gson
    gson.toJson(event)
  }

  // write event in json string format accepted by mongoDB (basically, json string with more quotation marks"
  def writeEventForStorage(event : Event): String = {
    val jsonString =  "\"" + "{\"row\":" + event.row + ",\"lat\":" + event.lat + ",\"long\":" + event.long + ",\"first_name\":" + "\"" + event.first_name + "\"" + ",\"last_name\":" + "\"" + event.last_name + "\"" + ",\"score\":" + event.score + "}" + "\""
    jsonString
  }

  // read json string in event format
  def readEvent(event: String): Event = {
    val gson = new Gson
    gson.fromJson(event, classOf[Event])
  }
}


