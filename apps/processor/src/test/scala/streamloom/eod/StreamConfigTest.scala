package streamloom.eod

import org.apache.kafka.streams.StreamsConfig
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test

class StreamConfigTest {
  @Test def utiliseLesValeursParDefaut(): Unit = {
    assertEquals(StreamConfig(), StreamConfig.fromEnvironment(Map.empty))
  }

  @Test def appliqueToutesLesVariablesEnvironnement(): Unit = {
    val config = StreamConfig.fromEnvironment(Map(
      "BROKERS" -> "broker:9092", "APP_ID" -> "demo", "INPUT_TOPIC" -> "input",
      "OUTPUT_1H_TOPIC" -> "hourly", "OUTPUT_1M_TOPIC" -> "minute", "OUTPUT_DAILY_TOPIC" -> "daily", "DLT_TOPIC" -> "errors"
    ))
    assertEquals(StreamConfig("broker:9092", "demo", "input", "minute", "daily", "errors", "hourly"), config)
    assertEquals("demo", config.properties.getProperty(StreamsConfig.APPLICATION_ID_CONFIG))
    assertEquals("broker:9092", config.properties.getProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG))
  }

  @Test def ignoreLesVariablesVides(): Unit = {
    val config = StreamConfig.fromEnvironment(Map("BROKERS" -> " ", "INPUT_TOPIC" -> "", "OUTPUT_1H_TOPIC" -> " "))
    assertEquals(StreamConfig(), config)
  }
}
