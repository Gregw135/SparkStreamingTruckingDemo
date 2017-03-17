package main.scala

import java.util.Properties

import com.orendainx.hortonworks.trucking.common.models.TruckEventTypes
import models._
import com.typesafe.config.ConfigFactory
import kafka.serializer.StringDecoder
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.spark.mllib.classification.LogisticRegressionModel
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.kafka._
import org.apache.spark.streaming._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.storage._
import org.apache.spark.mllib.classification.LogisticRegressionModel
import org.apache.spark.mllib.classification.LogisticRegressionWithSGD
import org.apache.spark.mllib.regression.StreamingLinearRegressionWithSGD
import org.apache.spark.streaming.dstream.DStream

case class CollectOptions(
  windowFrequency: Integer,
  streamingWindow: Integer,
  windowSlideDuration: Integer,
  kafkaBrokerList: String,
  truckingTopic: Set[String],
  trafficTopic: Set[String],
  enrichedTopic: String,
  windowedTopic: String,
  appName:String,
  modelLocation:String,
  numModelWeights:Integer
                         )

/** Setup Spark Streaming */
object Collect {
  private implicit val config = ConfigFactory.load()
  def main(args: Array[String]) {

    val options = new CollectOptions(
      config.getInt("spark.windowFrequency"),
      config.getInt("spark.streamingWindow"),
      config.getInt("spark.windowSlideDuration"),
      config.getString("spark.kafkaBrokerList"),
      Set[String](config.getString("spark.kafkaTopics.trucking")),
      Set[String](config.getString("spark.kafkaTopics.traffic")),
      config.getString("spark.kafkaTopics.joined"),
      config.getString("spark.kafkaTopics.windowed"),
      config.getString("spark.appName"),
      config.getString("spark.modelLocation"),
      config.getString("spark.numModelWeights").toInt
    )

    val conf = new SparkConf() //configuration properties will be passed in at runtime
    conf.set("spark.streaming.concurrentJobs", "4")
    conf.setAppName(options.appName)
    conf.setMaster("local")
    val sc = new SparkContext(conf)

    sc.setCheckpointDir("/tmp/SparkStreaming")
    sc.setLogLevel("ERROR")
    val ssc = new StreamingContext(sc, Seconds(options.streamingWindow.toLong))

    //Load pretrained model from HDFS, if it exists
    var logisticModel: LogisticRegressionModel = null
    var weights:org.apache.spark.mllib.linalg.Vector = null
    if(options.modelLocation != null && options.modelLocation != "") {
      try {
        logisticModel = LogisticRegressionModel.load(sc, options.modelLocation)
        weights = logisticModel.weights
      }catch{
        case e:Exception => {
          println("Exception: couldn't load Logistic Regression Model. Have you built one yet with Zeppelin?")
          e.printStackTrace()
        }
      }
    }
    Collector.doIt(options, sc, ssc, weights)
  }
}

/** Pull trucking and traffic events from Kafka */
object Collector extends App{

  def doIt(options: CollectOptions, sc: SparkContext, ssc: StreamingContext, weights: org.apache.spark.mllib.linalg.Vector) {

    val kafkaParams = Map(
      "metadata.broker.list" -> options.kafkaBrokerList
    )

    //Push back to Kafka
    val kafkaProps = new Properties()
    //props.put("metadata.broker.list",  options.kafkaBrokerList)
    kafkaProps.put("bootstrap.servers", options.kafkaBrokerList)
    kafkaProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    kafkaProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

    val trafficDStream = KafkaUtils.createDirectStream[
      String, String, StringDecoder, StringDecoder ](
      ssc, kafkaParams, options.trafficTopic)

    val cleanedTrafficStream = trafficDStream.map({
      record => {
        val Array(eventTime, routeId, congestionLevel) = record._2.split("\\|")
        (routeId.toInt, TrafficData(eventTime.toLong, routeId.toInt, congestionLevel.toInt))
      }
    })

    val truckEvents = KafkaUtils.createDirectStream[
      String, String, StringDecoder, StringDecoder ](
      ssc, kafkaParams, options.truckingTopic)

    val cleanedTruckEvents:DStream[(Int, EnrichedTruckData)] = truckEvents.map({
      record => {
          val Array(eventTime, truckId, driverId, driverName, routeId, routeName, latitude, longitude, speed, eventType, foggy, rainy, windy, unknown) = record._2.split("\\|")
          val tuple = (routeId.toInt, EnrichedTruckData(eventTime.toLong, truckId.toInt, driverId.toInt, driverName, routeId.toInt, routeName,
            latitude.toDouble, longitude.toDouble, speed.toInt, eventType, foggy.toInt, rainy.toInt, windy.toInt))
        tuple
      }
    })

    //Combine traffic and truck event data
    val joined = cleanedTruckEvents.join(cleanedTrafficStream)
    val truckAndTrafficData = joined.map(
      //entry: (routeId, (EnrichedTruckData, TrafficData))
      entry => {
        val truck: EnrichedTruckData = entry._2._1
        val traffic: TrafficData = entry._2._2
        val enriched = EnrichedTruckAndTrafficData(truck.eventTime, truck.truckId, truck.driverId, truck.driverName, truck.routeId, truck.routeName, truck.latitude,
          truck.longitude, truck.speed, truck.eventType, truck.foggy, truck.rainy, truck.windy, traffic.congestionLevel)
        //println("enriched trucking event. Route: " + enriched.routeId + " " + enriched.driverName)
        (enriched.driverId, enriched)
      }
    )
    joined.print()

    //Push joined data back to Kafka
    truckAndTrafficData.foreachRDD({
      rdd => {
        //KafkaProducer can't be serialized, so we have to create it locally in each executor using foreachPartition
        rdd.foreachPartition({
          partition => {
            val producer = new KafkaProducer[String, String](kafkaProps)
            partition.foreach({
              data =>
                val toKafka = data._2.toCSV
                val message = new ProducerRecord[String, String](options.enrichedTopic, null, toKafka)
                //println("sending enriched kafka: " + toKafka)
                producer.send(message)
            })
            producer.close()
          }
        })
      }
    })

    //send joined data to HDFS. Data can be analyzed with the provided Zeppelin notebook.
    truckAndTrafficData.map(
      //extract data from tuple of (driverId, data)
      rdd => rdd._2.toCSV
    ).foreachRDD(
      rdd => {
        rdd.saveAsTextFile("hdfs:///tmp/trucking/data/truckEvents/")
      }
    )

    val windowed = truckAndTrafficData.map(
      data =>{
        val enriched = data._2
        //driverId|$averageSpeed|$totalFog|$totalRain|$totalWind|$totalViolations"
        val isViolation = (if (enriched.eventType  == TruckEventTypes.Normal) 0 else 1)
        (enriched.driverId, WindowedDriverStats(enriched.driverId, enriched.speed, enriched.foggy, enriched.rainy, enriched.windy, isViolation, 1))
      }
    ).reduceByKeyAndWindow(
      (a:WindowedDriverStats,b:WindowedDriverStats) => {
        //weight stats by # of events
        val numA = a.totalEvents
        val numB = b.totalEvents
        val avgSpeed = (a.averageSpeed * numA + b.averageSpeed * numB) / (numA + numB)
        val avgFog = (a.fog*numA + b.fog*numB)/(numA + numB)
        val avgRain = (a.rain*numA + b.rain*numB)/(numA + numB)
        val avgWind = (a.wind*numA + b.wind*numB)/(numA + numB)
        val avgViolation = (a.totalViolations*numA + b.totalViolations*numB)/(numA + numB)
        //println("aEvents: " + numA + " B: " + numB + " speed: " + avgSpeed + " fog: " + avgFog + " rain: " + avgRain + " wind: " + avgWind + " violation:" + avgViolation)
        WindowedDriverStats(a.driverId, avgSpeed, avgFog, avgRain, avgWind, avgViolation, (numA + numB))
      }, Seconds(options.windowSlideDuration.toLong), Seconds(options.windowFrequency.toLong))

  //send to Kafka
    windowed.foreachRDD({
      rdd => {
        //KafkaProducer can't be serialized, so we have to create it locally in each executor using foreachPartition
        rdd.foreachPartition({
          partition => {
            val producer = new KafkaProducer[String, String](kafkaProps)
            partition.foreach({
              data =>
                val toKafka = data._2.toCSV
                val message = new ProducerRecord[String, String](options.windowedTopic, null,toKafka)
                //println("sending windowed message: " + toKafka)
                producer.send(message)
            })
            producer.close()
          }
        })
      }
    })

    //send windowed data to HDFS
    windowed.map(
      //extract data from tuple of (driverId, data)
      rdd => rdd._2.toCSV
    ).foreachRDD(
      rdd => rdd.saveAsTextFile("hdfs:///tmp/trucking/data/windowedEvents/")
    )

    //make predictions on each event
    val predictor = new Predictor(weights, options.numModelWeights)
    val predictionStream = predictor.predict(truckAndTrafficData.map(row => row._2  ))
    //print predictions
    predictionStream.foreachRDD(
      rdd =>
        rdd.foreach(row => println(row))
    )
    predictionStream.print()

    //Train the model
    predictor.train(truckAndTrafficData.map(row => row._2 ))


    //truckAndTrafficData.print()
    ssc.start()
    ssc.awaitTermination()
  }

//  def trackTrafficFunc(time: Time, routeId: String, value: Option[String], state: State[(Int, TrafficData)]): Option[(Int, TrafficData)] = {
//    val data:String = value.get
//    val Array(eventTime, routeId, congestionLevel) = data.split("\\|")
//    val tData = TrafficData(eventTime.toLong, routeId.toInt, congestionLevel.toInt)
//    state.update((tData.routeId, tData))
//    println("updated traffic state: " + routeId + " " + congestionLevel)
//    Some((tData.routeId, tData))
//  }

}
