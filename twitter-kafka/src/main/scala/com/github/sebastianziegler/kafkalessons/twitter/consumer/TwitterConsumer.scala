package com.github.sebastianziegler.kafkalessons.twitter.consumer

import java.time.Duration
import java.util.Properties

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecords, KafkaConsumer}
import org.apache.kafka.common.serialization.StringDeserializer
import org.slf4j.LoggerFactory

import scala.jdk.CollectionConverters._

class Consumer {
  private val logger = LoggerFactory.getLogger(classOf[Consumer])
  private val TOPIC_NAME = "test-topic"
  private val GROUP_ID = "test-group"
  val properties = getProperties()

  def getData(): Unit = {
    val consumer = new KafkaConsumer[String, String](properties)

    consumer.subscribe(List(TOPIC_NAME).asJava)

    val records: ConsumerRecords[String, String] = consumer.poll(Duration.ofSeconds(1))

    consumer.close()

    records.records(TOPIC_NAME).forEach { record =>
      logger.info(s"Message received: ${record.key()}|${record.value()}")
    }
  }

  def getProperties(): Properties = {
    val properties = new Properties()

    properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)
    properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)
    properties.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID)
    properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")

    properties
  }
}

object ConsumerApp extends App {
  val consumer = new Consumer
  consumer.getData()
}