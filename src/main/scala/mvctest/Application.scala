package mvctest

object Application  {
  //SpringApplication.run(classOf[BootConfig]);


  // Main method
  def main(args: Array[String])
  {
    // call to submit task to spark
    KafkaSparkPopularHashTags.computeTopHash(Array[String]("localhost:2181", "spark-streaming-consumer-group", "tweets", "4"))
  }
}