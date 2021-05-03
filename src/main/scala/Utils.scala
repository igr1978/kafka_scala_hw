import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.dataformat.csv.{CsvMapper, CsvSchema}

import org.json4s._
import org.json4s.JsonDSL._
import org.json4s.CustomSerializer

import java.io.File

object Utils {

  val topic = "books"
  val csv_path = "src/main/resources/bestsellers_with_categories.csv"
  val msg_count = 5

  implicit val formats = DefaultFormats + new BookSerializer()

  case class Book(
                   name: String,
                   author: String,
                   rating: Float,
                   reviews: Long,
                   price: Int,
                   year: Int,
                   genre: String
                 )

  class BookSerializer extends CustomSerializer[Book](format => ( {
    case jsonObj: JObject =>
      val name = (jsonObj \ "name").extract[String]
      val author = (jsonObj \ "author").extract[String]
      val rating = (jsonObj \ "rating").extract[String].toFloat
      val reviews = (jsonObj \ "reviews").extract[String].toLong
      val price = (jsonObj \ "price").extract[String].toInt
      val year = (jsonObj \ "year").extract[String].toInt
      val genre = (jsonObj \ "genre").extract[String]

      Book(name, author, rating, reviews, price, year, genre)
  }, {
    case book: Book =>
      ("name" -> book.name) ~
        ("author" -> book.author) ~
        ("rating" -> book.rating) ~
        ("reviews" -> book.reviews) ~
        ("price" -> book.price) ~
        ("year" -> book.year) ~
        ("genre" -> book.genre)
  }
  ))

  def csvFileToJson(filePath: String) = {
    val inputCsvFile = new File(filePath)

    // if the csv has header, use setUseHeader(true)
    val csvSchema = CsvSchema.builder()
      .addColumn("name", CsvSchema.ColumnType.STRING)
      .addColumn("author", CsvSchema.ColumnType.STRING)
      .addColumn("rating", CsvSchema.ColumnType.NUMBER)
      .addColumn("reviews", CsvSchema.ColumnType.NUMBER)
      .addColumn("price", CsvSchema.ColumnType.NUMBER)
      .addColumn("year", CsvSchema.ColumnType.NUMBER)
      .addColumn("genre", CsvSchema.ColumnType.STRING)
      .setUseHeader(true)
      .build()

    val csvMapper = new CsvMapper()

    // java.util.Map[String, String] identifies they key values type in JSON
    val readAll = csvMapper.readerFor(classOf[java.util.Map[String, String]]).`with`(csvSchema).readValues(inputCsvFile).readAll()
    val mapper = new ObjectMapper()

    // json return value
    mapper.writerWithDefaultPrettyPrinter().writeValueAsString(readAll)
    //mapper.writeValueAsString(readAll)
  }

}
