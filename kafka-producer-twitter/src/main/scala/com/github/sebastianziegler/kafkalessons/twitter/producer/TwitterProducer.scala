package com.github.sebastianziegler.kafkalessons.twitter.producer

import java.util.Properties
import java.util.concurrent.LinkedBlockingQueue

import com.github.sebastianziegler.kafkalessons.twitter.TwitterFactory
import com.twitter.hbc.core.Client
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord, RecordMetadata}
import org.apache.kafka.common.serialization.StringSerializer
import org.slf4j.LoggerFactory

class TwitterProducer(
                       producer: KafkaProducer[String, String],
                       msgQueue: LinkedBlockingQueue[String],
                       twitterClient: Client
                     ) {
  private val logger = LoggerFactory.getLogger(classOf[TwitterProducer])
  private val TOPIC_NAME = "twitter-topic"

  def harvestTweets(): Unit = {
    twitterClient.connect()

    while (!twitterClient.isDone){
      val msg = msgQueue.take()
      logger.info(s"Received: $msg")
      val record = new ProducerRecord[String, String](TOPIC_NAME, msg)

      producer.send(record, (metadata: RecordMetadata, exception: Exception) => {
        if (exception != null) {
          logger.error(s"ERROR ${exception.getMessage}")
        } else {
          logger.info(s"sent ${metadata.toString}")
        }
      })
    }
  }
}

object ProducerApp extends App {
  val properties = getProperties()
  private val logger = LoggerFactory.getLogger(classOf[TwitterProducer])
  private val kafkaClient = new KafkaProducer[String, String](properties)
  private val msgQueue = new LinkedBlockingQueue[String](100000)
  private val twitterClient = TwitterFactory.getClient(List("bitcoin"), msgQueue)
  val producer = new TwitterProducer(kafkaClient, msgQueue, twitterClient)

  sys.addShutdownHook{
    logger.info("Finalizing jobs")
    twitterClient.stop()
    kafkaClient.flush()
    kafkaClient.close()
  }

  producer.harvestTweets()

  def getProperties(): Properties = {
    val properties = new Properties()

    //producer config
    properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
    properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)

    //safe producer
    properties.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true")
    properties.put(ProducerConfig.ACKS_CONFIG, "all")
    properties.put(ProducerConfig.RETRIES_CONFIG, Integer.MAX_VALUE.toString)
    properties.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5")

    //throughput improvements
    properties.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy")
    properties.put(ProducerConfig.LINGER_MS_CONFIG, "20")
    properties.put(ProducerConfig.BATCH_SIZE_CONFIG, (32*1024).toString)

    properties
  }
}