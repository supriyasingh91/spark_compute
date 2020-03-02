package mvctest

import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.kafka._
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object KafkaSparkPopularHashTags {


  val conf = new SparkConf().setMaster("spark://192.168.1.10:7077").setAppName("Spark Streaming - Kafka Producer - PopularHashTags").set("spark.executor.memory", "1g")
  val sc = new SparkContext(conf)


  def computeTopHash(args: Array[String]) {

    sc.setLogLevel("WARN")

    // Create an array of arguments: zookeeper hostname/ip,consumer group, topicname, num of threads
    val Array(zkQuorum, group, topics, numThreads) = args

    // Set the Spark StreamingContext to create a DStream for every 2 seconds
    val ssc = new StreamingContext(sc, Seconds(2))
    ssc.checkpoint("checkpoint")

    // Map each topic to a thread
    val topicMap = topics.split(",").map((_, numThreads.toInt)).toMap
    // Map value from the kafka message (k, v) pair
    val lines = KafkaUtils.createStream(ssc, zkQuorum, group, topicMap).map(_._2)
    // Filter hashtags
    val hashTags = lines.flatMap(_.split(" ")).filter(_.startsWith("#"))

    // Get the top hashtags over the previous 60/10 sec window
    val topCounts60 = hashTags.map((_, 1)).reduceByKeyAndWindow(_ + _, Seconds(60))
      .map { case (topic, count) => (count, topic) }
      .transform(_.sortByKey(false))

    val topCounts10 = hashTags.map((_, 1)).reduceByKeyAndWindow(_ + _, Seconds(10))
      .map { case (topic, count) => (count, topic) }
      .transform(_.sortByKey(false))

    lines.print()

    // Print popular hashtags
    topCounts60.foreachRDD(rdd => {
      val topList = rdd.take(10)
      println("\nPopular topics in last 60 seconds (%s total):".format(rdd.count()))
      topList.foreach { case (count, tag) => println("%s (%s tweets)".format(tag, count)) }
    })

    topCounts10.foreachRDD(rdd => {
      val topList = rdd.take(10)
      println("\nPopular topics in last 10 seconds (%s total):".format(rdd.count()))
      topList.foreach { case (count, tag) => println("%s (%s tweets)".format(tag, count)) }
    })

    lines.count().map(cnt => "Received " + cnt + " kafka messages.").print()

    ssc.start()
    ssc.awaitTermination()
  }

}
