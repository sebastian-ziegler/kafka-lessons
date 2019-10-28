package com.github.sebastianziegler.kafkalessons.elasticsearch.consumer

import java.time.Duration
import java.util.Properties

import com.github.sebastianziegler.kafkalessons.elasticsearch.ElasticSearchFactory
import com.google.gson.JsonParser
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecords, KafkaConsumer}
import org.apache.kafka.common.serialization.StringDeserializer
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.client.{RequestOptions, RestHighLevelClient}
import org.elasticsearch.common.xcontent.XContentType
import org.slf4j.LoggerFactory

import scala.jdk.CollectionConverters._

class ConsumerElasticsearch(
                elasticClient: RestHighLevelClient,
                kafkaConsumer: KafkaConsumer[String, String]) {
  private val logger = LoggerFactory.getLogger(classOf[ConsumerElasticsearch])
  private val TOPIC_NAME = "twitter-topic"

  private def extractIdFromTweet(str: String): String = {
    JsonParser.parseString(str).getAsJsonObject.get("id_str").getAsString
  }

  def consumeTweets(): Unit = {

    kafkaConsumer.subscribe(List(TOPIC_NAME).asJava)

    while (true) {
      val records: ConsumerRecords[String, String] = kafkaConsumer.poll(Duration.ofSeconds(1))

      records.records(TOPIC_NAME).forEach { record =>
        logger.info(s"Message received: ${record.key()}|${record.value()}")
        val twitterId = extractIdFromTweet(record.value())
        val indexRequest = new IndexRequest("twitter", "tweets", twitterId).source(record.value, XContentType.JSON)
        val idxResponse = elasticClient.index(indexRequest, RequestOptions.DEFAULT)

        logger.info(s"Inserting into elasticSearch id is: ${idxResponse.getId}")
      }
    }

    kafkaConsumer.close()
    elasticClient.close()
  }
}

object ConsumerApp extends App {
  val logger = LoggerFactory.getLogger(classOf[ConsumerElasticsearch])
  val elasticsearchClient = ElasticSearchFactory.getClient()
  val GROUP_ID = "kafka-demo-elasticsearch"
  val properties = getProperties()
  val kafkaConsumer = new KafkaConsumer[String, String](properties)
  val consumer = new ConsumerElasticsearch(elasticsearchClient, kafkaConsumer)

  sys.addShutdownHook{
    logger.info("Finalizing jobs")
    kafkaConsumer.close()
    elasticsearchClient.close()
  }

  consumer.consumeTweets()

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