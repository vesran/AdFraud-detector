/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.myorg.quickstart

import org.apache.flink.api.common.functions.RuntimeContext

import java.util.Properties
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer
import org.elasticsearch.action.index.IndexRequest
import org.apache.http.HttpHost
import org.apache.flink.streaming.connectors.elasticsearch6.{ElasticsearchSink, RestClientFactory}
import org.elasticsearch.client.{Requests, RestClientBuilder}

/**
 * Skeleton for a Flink Streaming Job.
 *
 * For a tutorial how to write a Flink streaming application, check the
 * tutorials and examples on the <a href="https://flink.apache.org/docs/stable/">Flink Website</a>.
 *
 * To package your application into a JAR file for execution, run
 * 'mvn clean package' on the command line.
 *
 * If you change the name of the main class (with the public static void main(String[] args))
 * method, change the respective entry in the POM.xml file (simply search for 'mainClass').
 */
object StreamingJob {
  def main(args: Array[String]) {
    // set up the streaming execution environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    /*
     * Here, you can start creating your execution plan for Flink.
     *
     * Start with getting some data from the environment, like
     *  env.readTextFile(textPath);
     *
     * then, transform the resulting DataStream[String] using operations
     * like
     *   .filter()
     *   .flatMap()
     *   .join()
     *   .group()
     *
     * and many more.
     * Have a look at the programming guide:
     *
     * https://flink.apache.org/docs/latest/apis/streaming/index.html
     *
     */


    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "localhost:9092")
    // only required for Kafka 0.8
    properties.setProperty("zookeeper.connect", "localhost:2181")
    properties.setProperty("group.id", "test")

    // Changer le SimpleStringSchema pour lire du JSON
    val stream = env.addSource(new FlinkKafkaConsumer[String]("displays", new SimpleStringSchema(), properties))
    val otherStream = env.addSource(new FlinkKafkaConsumer[String]("clicks", new SimpleStringSchema(), properties))

    // Code pour sink
    val httpHosts = new java.util.ArrayList[HttpHost]
    httpHosts.add(new HttpHost("localhost", 9200, "http"))

    val esSinkBuilder = new ElasticsearchSink.Builder[String](
      httpHosts,
      new ElasticsearchSinkFunction[String] {
        def process(element: String, ctx: RuntimeContext, indexer: RequestIndexer) {
          val json = new java.util.HashMap[String, Array[String]]
          val parsedElement = element.slice(1,element.length-1).split(",")
          json.put("data",parsedElement)
          json.put("timestamp",Array(parsedElement(2)))

          val rqst: IndexRequest = Requests.indexRequest
            .index("displays-clicks-idx")
            .`type`("interactions")
            .source(json)


          indexer.add(rqst)
        }
      }
    )

    // configuration for the bulk requests; this instructs the sink to emit after every element, otherwise they would be buffered
    esSinkBuilder.setBulkFlushMaxActions(1)

    stream.addSink(esSinkBuilder.build)
    otherStream.addSink(esSinkBuilder.build)

    stream.print()
    otherStream.print()

    env.execute("Flink Streaming Scala API Skeleton")
  }
}
