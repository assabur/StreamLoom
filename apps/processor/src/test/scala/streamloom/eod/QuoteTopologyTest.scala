package streamloom.eod

import java.time.Instant
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test

class QuoteTopologyTest {
  @Test def respecteLesNomsDeTopicsPersonnalises(): Unit = {
    val kafka = new TopologyFixture(StreamConfig(inputTopic = "quotes", output1mTopic = "minute",
      output1hTopic = "hourly", outputDailyTopic = "daily", dltTopic = "errors"))
    try {
      kafka.send(Json.write(QuoteExamples.apple))
      assertTrue(kafka.minuteIsEmpty)
      kafka.advanceTo(121)
      val (key, aggregate) = kafka.readMinutes().head
      assertEquals("AAPL.US", key)
      assertEquals(1L, aggregate.count)
      assertEquals("2026-11-12T00:00:00Z", aggregate.windowStart)
      kafka.advanceTo(3901)
      assertEquals("1h", kafka.readHours().head._2.windowLabel)
      // La fenêtre suit le timestamp Kafka, pas la date 15:30 dans le JSON.
      assertEquals(Instant.parse("2026-11-12T15:30:00Z").toEpochMilli, aggregate.firstEventTimeMs)
    } finally kafka.close()
  }

  @Test def dirigeUnTombstoneVersLaDlt(): Unit = {
    val kafka = new TopologyFixture()
    try {
      kafka.send(null)
      val error = kafka.readError()
      assertNull(error.raw)
      assertEquals("empty_payload", error.error)
      assertTrue(kafka.minuteIsEmpty)
      assertTrue(kafka.dailyIsEmpty)
      assertTrue(kafka.hourlyIsEmpty)
    } finally kafka.close()
  }

  @Test def rejetteUnMessageArriveApresLeDelaiDeGrace(): Unit = {
    val kafka = new TopologyFixture()
    try {
      kafka.send(Json.write(QuoteExamples.apple))
      kafka.advanceTo(121)
      assertEquals(1, kafka.readMinutes().size)
      kafka.send(Json.write(QuoteExamples.apple.copy(volume = 999)), seconds = 30)
      assertTrue(kafka.minuteIsEmpty)
    } finally kafka.close()
  }

  @Test def agregeLesCotationsDeLHeureParSymbole(): Unit = {
    val kafka = new TopologyFixture()
    try {
      kafka.send(Json.write(QuoteExamples.apple.copy(open = 100, high = 110, low = 90, close = 105, volume = 10)), 0)
      kafka.send(Json.write(QuoteExamples.apple.copy(open = 105, high = 120, low = 95, close = 115, volume = 20)), 3599)
      kafka.send(Json.write(QuoteExamples.apple.copy(symbol = "MSFT.US", volume = 7)), 3599)
      kafka.advanceTo(3901)
      val records = kafka.readHours().toMap
      assertEquals(Set("AAPL.US", "MSFT.US"), records.keySet)
      val aggregate = records("AAPL.US")
      assertEquals("1h", aggregate.windowLabel)
      assertEquals("2026-11-12T00:00:00Z", aggregate.windowStart)
      assertEquals("2026-11-12T01:00:00Z", aggregate.windowEnd)
      assertEquals(100.0, aggregate.open)
      assertEquals(120.0, aggregate.high)
      assertEquals(90.0, aggregate.low)
      assertEquals(115.0, aggregate.close)
      assertEquals(30L, aggregate.volumeSum)
      assertEquals(2L, aggregate.count)
      assertEquals(7L, records("MSFT.US").volumeSum)
    } finally kafka.close()
  }

  @Test def separeDeuxHeuresConsecutives(): Unit = {
    val kafka = new TopologyFixture()
    try {
      kafka.send(Json.write(QuoteExamples.apple.copy(volume = 10)), 3599)
      kafka.send(Json.write(QuoteExamples.apple.copy(volume = 20)), 3600)
      kafka.advanceTo(7501)
      val records = kafka.readHours().filter(_._1 == "AAPL.US").map(_._2).sortBy(_.windowStart)
      assertEquals(2, records.size)
      assertEquals(List(10L, 20L), records.map(_.volumeSum))
      assertEquals(List("2026-11-12T00:00:00Z", "2026-11-12T01:00:00Z"), records.map(_.windowStart))
    } finally kafka.close()
  }

  @Test def attendLaGraceHoraireEtRejetteLesMessagesApresFermeture(): Unit = {
    val kafka = new TopologyFixture()
    try {
      kafka.send(Json.write(QuoteExamples.apple.copy(volume = 10)), 0)
      kafka.advanceTo(3899)
      assertTrue(kafka.hourlyIsEmpty)
      // Une cotation retardée reste acceptée tant que la grâce n'est pas dépassée.
      kafka.send(Json.write(QuoteExamples.apple.copy(volume = 20)), 3599)
      kafka.advanceTo(3901)
      assertEquals(30L, kafka.readHours().find(_._1 == "AAPL.US").get._2.volumeSum)
      kafka.send(Json.write(QuoteExamples.apple.copy(volume = 999)), 3599)
      assertTrue(kafka.hourlyIsEmpty)
    } finally kafka.close()
  }

}
