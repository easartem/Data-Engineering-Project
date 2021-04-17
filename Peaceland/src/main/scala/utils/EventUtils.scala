package utils

import com.google.gson.Gson

object EventUtils {

  case class Event(
                    row: Int,
                    lat: Float,
                    long: Float,
                    first_name: String,
                    last_name: String,
                    score: Int
                   )
  {
    def getPeacescore(event : Event) : Int = {
      event.score
    }
  }

  def parseFromJson(lines: Iterator[String]): Iterator[Event] = {
    val gson = new Gson
    lines.map(line => gson.fromJson(line, classOf[Event]))
  }
}


