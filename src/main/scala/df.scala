import org.apache.spark.sql.{SparkSession, DataFrame}
import requests._
import ujson._

object df extends App {
  val spark = SparkSession.builder()
    .appName("df")
    .master("local[*]")
    .getOrCreate()

  // Fetch data from API
  val apiUrl = "https://freetestapi.com/api/v1/students"
  val response = requests.get(apiUrl)
  val jsonData = response.text()

  // Parse and pretty-print JSON data
  val parsedJson = ujson.read(jsonData)
  val prettyJson = ujson.write(parsedJson, indent = 4)

  // Save the JSON data to a local file
  val localPath = "students.json"
  import java.io._
  val pw = new PrintWriter(new File(localPath))
  pw.write(prettyJson)
  pw.close()

  // Read the saved JSON file into a DataFrame
  val df: DataFrame = spark.read.option("multiline", "true").json(localPath)

  // Print the schema of the DataFrame
  df.printSchema()

  // Show the contents of the DataFrame
  df.show()

  // Perform some aggregations
  // 1. Count the number of students by gender
  df.groupBy("gender").count().show()

  // 2. Calculate the average age of students
  df.selectExpr("avg(age) as avg_age").show()

  // 3. Count the number of students by state (if address is a nested structure)
  df.selectExpr("address.state").groupBy("state").count().show()

  // Stop the Spark session
  spark.stop()
}
