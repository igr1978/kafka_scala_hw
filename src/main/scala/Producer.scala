import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringSerializer

import org.json4s._
import org.json4s.native.JsonMethods._
import org.json4s.native.Serialization

import java.util.Properties

object Producer extends App {
  import Utils._
//  implicit val formats = DefaultFormats
  implicit val formats = DefaultFormats + new BookSerializer()


  val props = new Properties()
  props.put("bootstrap.servers", "localhost:29092")

  val jsonFile = csvFileToJson(csv_path)
  val parsedJson = parse(jsonFile)
  val records = parsedJson.extract[List[Book]]

//  val records = parsedJson.transformField {
//    case ("Name", x) => ("name", x)
//    case ("Author", x) => ("author", x)
//    case ("User Rating", x) => ("rating", x)
//    case ("Reviews", x) => ("reviews", x)
//    case ("Price", x) => ("price", x)
//    case ("Year", x) => ("year", x)
//    case ("Genre", x) => ("genre", x)
//  }.extract[List[Book]]

  val producer = new KafkaProducer(props, new StringSerializer, new StringSerializer)
  try {
    records.zipWithIndex.foreach { r =>
      val msg_out: String = Serialization.write(r._1)(DefaultFormats)
      val record = new ProducerRecord[String, String](topic, r._2.toString, msg_out)

      val metadata = producer.send(record)

      printf(s"sent record: (key=${record.key} value=${record.value}), meta(partition=${metadata.get.partition}, offset=${metadata.get.offset})\n")

//      printf(s"sent record: (key=%s value=%s) meta(partition=%d, offset=%d)\n",
//        record.key(), record.value(), metadata.get().partition(), metadata.get().offset())
    }
  }
  catch {
    case e:Exception => e.printStackTrace()
      sys.exit(-1)
  }
  finally {
    producer.close()
  }

  sys.exit(0)
}
