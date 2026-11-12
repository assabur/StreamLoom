package streamloom.eod

import java.nio.file.Files
import java.time.{Clock, Instant, ZoneOffset}
import java.util.UUID
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}
import org.apache.kafka.streams.{StreamsConfig, TopologyTestDriver}
import scala.jdk.CollectionConverters._

// Un Kafka en mémoire pour les tests : aucun broker et aucune attente réelle.
final class TopologyFixture(val config: StreamConfig = StreamConfig()) extends AutoCloseable {
  val start: Instant = Instant.parse("2026-11-12T00:00:00Z")
  private val stateDir = Files.createTempDirectory("streamloom-test-")
  private val clock = Clock.fixed(start, ZoneOffset.UTC)
  private val properties = config.properties
  properties.put(StreamsConfig.APPLICATION_ID_CONFIG, s"test-${UUID.randomUUID()}")
  properties.put(StreamsConfig.STATE_DIR_CONFIG, stateDir.toString)
  properties.put(StreamsConfig.STATESTORE_CACHE_MAX_BYTES_CONFIG, "0")

  private val driver = new TopologyTestDriver(QuoteTopology.build(config, clock), properties, start)
  private val input = driver.createInputTopic(config.inputTopic, new StringSerializer(), new StringSerializer())
  private val minute = driver.createOutputTopic(config.output1mTopic, new StringDeserializer(), new StringDeserializer())
  private val hourly = driver.createOutputTopic(config.output1hTopic, new StringDeserializer(), new StringDeserializer())
  private val daily = driver.createOutputTopic(config.outputDailyTopic, new StringDeserializer(), new StringDeserializer())
  private val dlt = driver.createOutputTopic(config.dltTopic, new StringDeserializer(), new StringDeserializer())

  def send(raw: String, seconds: Long = 0): Unit = input.pipeInput("original-key", raw, start.plusSeconds(seconds))

  def advanceTo(seconds: Long): Unit =
    send(Json.write(QuoteExamples.apple.copy(symbol = "CLOCK")), seconds)

  def minuteIsEmpty: Boolean = minute.isEmpty
  def hourlyIsEmpty: Boolean = hourly.isEmpty
  def dailyIsEmpty: Boolean = daily.isEmpty
  def dltIsEmpty: Boolean = dlt.isEmpty
  def readError(): DltRecord = Json.read(dlt.readValue(), classOf[DltRecord])
  def readMinutes(): List[(String, OhlcvAggregate)] = minute.readKeyValuesToList().asScala.toList
    .map(record => record.key -> Json.read(record.value, classOf[OhlcvAggregate]))
  def readHours(): List[(String, OhlcvAggregate)] = hourly.readKeyValuesToList().asScala.toList
    .map(record => record.key -> Json.read(record.value, classOf[OhlcvAggregate]))
  def readDays(): List[(String, OhlcvAggregate)] = daily.readKeyValuesToList().asScala.toList
    .map(record => record.key -> Json.read(record.value, classOf[OhlcvAggregate]))

  override def close(): Unit = {
    try driver.close()
    finally {
      val paths = Files.walk(stateDir)
      try paths.iterator().asScala.toList.sortBy(_.getNameCount).reverse.foreach(Files.deleteIfExists(_))
      finally paths.close()
    }
  }
}
