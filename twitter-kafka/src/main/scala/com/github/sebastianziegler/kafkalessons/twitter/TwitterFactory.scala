package com.github.sebastianziegler.kafkalessons.twitter

import java.util.concurrent.BlockingQueue

import com.twitter.hbc.ClientBuilder
import com.twitter.hbc.core.{Client, Constants, HttpHosts}
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint
import com.twitter.hbc.core.processor.StringDelimitedProcessor
import com.twitter.hbc.httpclient.auth.OAuth1
import com.typesafe.config.ConfigFactory

import scala.jdk.CollectionConverters._

object TwitterFactory {
  def getClient(topics: List[String], msgQueue: BlockingQueue[String]): Client = {
    val config = ConfigFactory.load("credentials.conf").getConfig("twitter")
    val hosebirdHosts = new HttpHosts(Constants.STREAM_HOST)
    val hosebirdEndpoint = new StatusesFilterEndpoint()
    val auth = new OAuth1(
      config.getString("apikey"),
      config.getString("apisecretkey"),
      config.getString("accesstoken"),
      config.getString("accesssecrettoken")
    )
    hosebirdEndpoint.trackTerms(topics.asJava)

    new ClientBuilder()
      .name("client-012")
      .hosts(hosebirdHosts)
      .authentication(auth)
      .endpoint(hosebirdEndpoint)
      .processor(new StringDelimitedProcessor(msgQueue))
      .build()
  }
}
