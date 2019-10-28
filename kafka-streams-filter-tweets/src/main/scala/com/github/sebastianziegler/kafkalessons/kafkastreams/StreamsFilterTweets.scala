package com.github.sebastianziegler.kafkalessons.kafkastreams

import java.util.Properties

import com.google.gson.JsonParser
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.kstream.KStream
import org.apache.kafka.streams.{KafkaStreams, StreamsBuilder, StreamsConfig}
import org.slf4j.LoggerFactory

class StreamsFilterTweets {
  val logger = LoggerFactory.getLogger(classOf[StreamsFilterTweets])
  private def extractFollowersFromTweet(str: String): Int = {
    Option(JsonParser.parseString(str).getAsJsonObject.get("user").getAsJsonObject.get("followers_count")).map(_.getAsInt).getOrElse(0)
  }

  def pullTweets(): Unit = {
    logger.info("Initializing properties")
    val properties = new Properties()
    properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092")
    properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "demo-kafka-streams")
    properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, classOf[Serdes.StringSerde].getName)
    properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, classOf[Serdes.StringSerde].getName)

    logger.info("Creating stream")
    val streamsBuilder = new StreamsBuilder()

    val inputTopic: KStream[String, String] = streamsBuilder.stream("twitter-topic")
    val filteredStreams: KStream[String, String] = inputTopic.filter((k, jsonTweet) => {
      extractFollowersFromTweet(jsonTweet) > 10000
    })

    filteredStreams.to("important_tweets")

    logger.info("Creating client")
    val kafkaStreams = new KafkaStreams(streamsBuilder.build(), properties)

    logger.info("Starting")
    kafkaStreams.start()
  }
}

object StreamsFilterTweets extends App {
  def streamsFilterTweets = new StreamsFilterTweets
  streamsFilterTweets.pullTweets()
}