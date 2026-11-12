package streamloom.eod

import java.util.Properties
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.StreamsConfig

final case class StreamConfig(
    brokers: String = "localhost:9092",
    appId: String = "eod-aggregator-streams",
    inputTopic: String = "eod",
    output1mTopic: String = "eod-agg-1m",
    outputDailyTopic: String = "eod-agg-daily",
    dltTopic: String = "eod-dlt",
    output1hTopic: String = "eod-agg-1h"
) {
  def properties: Properties = {
    val props = new Properties()
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, appId)
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, brokers)
    props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass)
    props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass)
    props
  }
}

object StreamConfig {
  def fromEnvironment(env: Map[String, String] = sys.env): StreamConfig = {
    def value(key: String, default: String): String =
      env.get(key).filterNot(_.isBlank).getOrElse(default)

    StreamConfig(
      brokers = value("BROKERS", "localhost:9092"),
      appId = value("APP_ID", "eod-aggregator-streams"),
      inputTopic = value("INPUT_TOPIC", "eod"),
      output1mTopic = value("OUTPUT_1M_TOPIC", "eod-agg-1m"),
      outputDailyTopic = value("OUTPUT_DAILY_TOPIC", "eod-agg-daily"),
      dltTopic = value("DLT_TOPIC", "eod-dlt"),
      output1hTopic = value("OUTPUT_1H_TOPIC", "eod-agg-1h")
    )
  }
}
