package streamloom.eod

import org.apache.kafka.streams.KafkaStreams

object App {
  def main(args: Array[String]): Unit = {
    val config = StreamConfig.fromEnvironment()
    val streams = new KafkaStreams(QuoteTopology.build(config), config.properties)
    sys.addShutdownHook(streams.close())
    streams.start()
  }
}
