import requests._
import ujson._
object student extends App {

    val url = "https://freetestapi.com/api/v1/students"

    val response = requests.get(url)
    val studentsData = ujson.read(response.text())

    // Pretty print the JSON data
    println(ujson.write(studentsData, indent = 4))

    // Extract and print all parameters for each student
    studentsData.arr.foreach { student =>
      println("Student Information:")
      student.obj.foreach { case (key, value) =>
        println(s"$key: $value")
      }
      println("-----")
    }
  }








