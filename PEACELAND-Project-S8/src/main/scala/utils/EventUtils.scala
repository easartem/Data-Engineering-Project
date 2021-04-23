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
                    score: Int,
                    words: String
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
  def writeEventForStorage(event : Event, id : Integer) = {
    val jsonString = "{\"drone_id\":" + id + ",\"lat\":" + event.lat + ",\"long\":" + event.long + ",\"first_name\":" + "\"" + event.first_name + "\"" + ",\"last_name\":" + "\"" + event.last_name + "\"" + ",\"score\":" + event.score + ",\"words\":" + "\"" + event.words + "\"" + "}"
    //val jsonString = "\"" + "{\"row\":" + event.row + ",\"lat\":" + event.lat + ",\"long\":" + event.long + ",\"first_name\":" + "\"" + event.first_name + "\"" + ",\"last_name\":" + "\"" + event.last_name + "\"" + ",\"score\":" + event.score + "}" + "\""
    jsonString
  }

  // read json string in event format
  def readEvent(event: String): Event = {
    val gson = new Gson
    gson.fromJson(event, classOf[Event])
  }
  /**
  def main(args: Array[String])
  {
    println("Hello World!")
    val ev = new Event(2,20.2.toFloat,24.2.toFloat, "Pierre", "jacques",20, "j'aime la galette")
    println(ev)
    println(writeEventForStorage(ev,1))
    val testXr = writeEventForStorage(ev,1)
    println(testXr)
    println("{\"row\":250,\"lat\":51.84175,\"long\":5.8715134,\"first_name\":\"Felix\",\"last_name\":\"Whitely\",\"score\":47}")
  }**/
}