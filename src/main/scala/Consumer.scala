import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.{PartitionInfo, TopicPartition}
import org.apache.kafka.common.serialization.StringDeserializer

import java.util
import java.time.Duration
import java.util.Properties

import scala.collection.JavaConverters.iterableAsScalaIterableConverter
import scala.collection.convert.ImplicitConversions.`collection AsScalaIterable`

object Consumer extends App {
  import Utils._

  val props = new Properties()
  props.put("bootstrap.servers", "localhost:29092")
  props.put("group.id", "consumer1")
//  props.put("enable.auto.commit", "true")
//  props.put("auto.commit.interval.ms", "1000")

  val consumer = new KafkaConsumer(props, new StringDeserializer, new StringDeserializer)
  val topics = List(topic)
  try {
    val partitionsInfo: util.List[PartitionInfo] = consumer.partitionsFor(topic)
    val partitions = new util.ArrayList[TopicPartition]
    if(partitionsInfo != null) {
      for (partition <- partitionsInfo.asScala) {
        partitions.add(new TopicPartition(partition.topic, partition.partition))
      }

      consumer.assign(partitions)
      consumer.seekToEnd(partitions)

      for (partition <- partitions) {
        consumer.seek(partition, consumer.position(partition) - msg_count)

        val records =  consumer.poll(Duration.ofSeconds(1))
        for(record <- records.asScala) {
          printf(s"get record: (topic=%s) " +
            "(key=%s value=%s) " +
            "meta(partition=%d, offset=%d)\n",
            record.topic(),
            record.key(), record.value(),
            record.partition(),record.offset())
        }
      }
    }
  }
  catch {
    case e:Exception => e.printStackTrace()
      sys.exit(-1)
  }
  finally {
    consumer.close()
  }

  sys.exit(0)
}
