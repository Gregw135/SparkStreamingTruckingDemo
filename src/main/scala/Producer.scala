package main.scala

import java.io.InputStream
import java.util
import java.util.Properties

import com.typesafe.config.ConfigFactory
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}


/**
  * Produces fake Kafka trucking messages for testing purposes.
  */

case class ProduceOptions(
                           frequency: Integer,
                           kafkaBrokerList: String,
                           kafkaTrucking: String,
                           kafkaTraffic: String,
                           appName:String)

object Producer {

  val stream : InputStream = getClass.getResourceAsStream("/sampleData.txt")
  val lines = scala.io.Source.fromInputStream( stream ).getLines
  val exampleData = new util.ArrayList[String]

  private implicit val config = ConfigFactory.load()
  def main(args: Array[String]) {


    val options = new ProduceOptions(
      config.getInt("spark.messageFrequency"),
      config.getString("spark.kafkaBrokerList"),
      config.getString("spark.kafkaTopics.trucking"),
      config.getString("spark.kafkaTopics.traffic"),
      config.getString("spark.appName")
    )
    print(options.kafkaBrokerList)

    val props = new Properties()
    props.put("bootstrap.servers", options.kafkaBrokerList)
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

    val producer = new KafkaProducer[String, String](props)


    // Send some messages
    var counter = 0
    while(true) {

      var line:String = null
      if(lines.hasNext){
        line = lines.next()
        exampleData.add(line)
      }else{
        exampleData.get(counter)
        counter +=1
        if(counter == exampleData.size())
          counter = 0
      }
      line = "" + line.trim()

      //println(line)
      //val message = new ProducerRecord[String, String](options.kafkaTrucking, null, line)

      val line2 = ("1486669632837|30|5|Edgar Orendain|5|Joplin to Kansas City|" + (60 + scala.util.Random.nextInt(60)) +"|"+
        scala.util.Random.nextInt(100) + "|" + (60 +  scala.util.Random.nextInt(20)) + "|Violation|1|" + scala.util.Random.nextInt(2) +"|3|" + scala.util.Random.nextInt(100))

      println("not working: " + line)
      println("working    : " + line2)
      println("\n")
      val message = new ProducerRecord[String, String](options.kafkaTrucking, null, line2)

      producer.send(message)


      val message2 = new ProducerRecord[String, String](options.kafkaTraffic, null, "1|5|" + scala.util.Random.nextInt(10))
      producer.send(message2)

      Thread.sleep(options.frequency.longValue())
    }
  }

}
