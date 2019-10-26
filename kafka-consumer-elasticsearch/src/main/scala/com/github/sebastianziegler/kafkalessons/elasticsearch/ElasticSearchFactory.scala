package com.github.sebastianziegler.kafkalessons.elasticsearch

import com.typesafe.config.ConfigFactory
import org.apache.http.HttpHost
import org.apache.http.auth.{AuthScope, UsernamePasswordCredentials}
import org.apache.http.client.CredentialsProvider
import org.apache.http.impl.client.BasicCredentialsProvider
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder
import org.elasticsearch.client.{RestClient, RestClientBuilder, RestHighLevelClient}

object ElasticSearchFactory {
  def getClient() = {
    val config = ConfigFactory.load("credentials.conf").getConfig("elasticsearch")
    val host = config.getString("url")
    val username = config.getString("user")
    val password = config.getString("pass")

    val credentialsProvider: CredentialsProvider = new BasicCredentialsProvider
    credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(username, password))

    val builder = RestClient.builder(new HttpHost(host, 443, "https")).setHttpClientConfigCallback((httpClientBuilder: HttpAsyncClientBuilder) => {
      httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider)
    })

    new RestHighLevelClient(builder)
  }
}
