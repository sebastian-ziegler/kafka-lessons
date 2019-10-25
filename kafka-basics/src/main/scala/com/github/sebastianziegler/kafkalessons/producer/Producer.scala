package com.github.sebastianziegler.kafkalessons.producer

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord, RecordMetadata}
import org.apache.kafka.common.serialization.StringSerializer
import org.slf4j.LoggerFactory

class Producer {
  val properties = getProperties()
  private val logger = LoggerFactory.getLogger(classOf[Producer])
  private val TOPIC_NAME = "test-topic"

  def sendData(data: String): Unit = {
    val producer = new KafkaProducer[String, String](properties)
    val record = new ProducerRecord[String, String](TOPIC_NAME, data)

    producer.send(record, (metadata: RecordMetadata, exception: Exception) => {
      if (exception != null) {
        logger.error(s"ERROR ${exception.getMessage}")
      } else {
        logger.info(s"sent ${metadata.toString}")
      }
    })

    producer.flush()
    producer.close()
  }

  def getProperties(): Properties = {
    val properties = new Properties()

    properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
    properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)

    properties
  }
}

object ProducerApp extends App {
  val producer = new Producer

  producer.sendData("testing 123")
  producer.sendData("testing 1234")
}