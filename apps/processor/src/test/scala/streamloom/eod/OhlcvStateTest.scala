package streamloom.eod

import java.time.Instant
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test

class OhlcvStateTest {
  @Test def laPremiereCotationInitialiseLaBougie(): Unit = {
    val quote = QuoteExamples.apple.copy(eventTimeMs = 1000)
    val state = OhlcvState().update(quote)
    assertEquals(100.0, state.open)
    assertEquals(110.0, state.high)
    assertEquals(90.0, state.low)
    assertEquals(105.0, state.close)
    assertEquals(10L, state.volumeSum)
    assertEquals(1L, state.count)
    assertEquals(1000L, state.firstEventTimeMs)
    assertEquals(1000L, state.lastEventTimeMs)
    assertEquals("stooq", state.source)
  }

  @Test def deuxCotationsCalculentLesExtremesEtLaSomme(): Unit = {
    val first = OhlcvState().update(QuoteExamples.apple.copy(eventTimeMs = 1000))
    val second = QuoteExamples.apple.copy(open = 105, high = 120, low = 80,
      close = 115, volume = 20, eventTimeMs = 2000)
    val result = first.update(second)

    assertEquals(100.0, result.open)
    assertEquals(120.0, result.high)
    assertEquals(80.0, result.low)
    assertEquals(115.0, result.close)
    assertEquals(30L, result.volumeSum)
    assertEquals(2L, result.count)
    assertEquals(1000L, result.firstEventTimeMs)
    assertEquals(2000L, result.lastEventTimeMs)
    assertEquals(1L, first.count) // L'ancien état reste inchangé.
  }

  @Test def uneCotationInterieureNeChangePasLesExtremes(): Unit = {
    val result = OhlcvState().update(QuoteExamples.apple)
      .update(QuoteExamples.apple.copy(high = 108, low = 95))
    assertEquals(110.0, result.high)
    assertEquals(90.0, result.low)
  }

  @Test def laClotureSuitLOrdreDeTraitement(): Unit = {
    val result = OhlcvState().update(QuoteExamples.apple.copy(eventTimeMs = 2000))
      .update(QuoteExamples.apple.copy(close = 99, eventTimeMs = 1000))
    assertEquals(99.0, result.close)
    assertEquals(2000L, result.firstEventTimeMs)
    assertEquals(1000L, result.lastEventTimeMs)
  }

  @Test def uneCotationRepeteeEstCompteeDeuxFois(): Unit = {
    val result = OhlcvState().update(QuoteExamples.apple).update(QuoteExamples.apple)
    assertEquals(20L, result.volumeSum)
    assertEquals(2L, result.count)
  }

  @Test def exporteLesBornesEtLeLabelDeLaFenetre(): Unit = {
    val now = Instant.parse("2026-11-12T16:00:00Z")
    val aggregate = OhlcvState().update(QuoteExamples.apple)
      .toAggregate("AAPL.US", 0, 60000, "1m", now)
    assertEquals("AAPL.US", aggregate.symbol)
    assertEquals("1970-01-01T00:00:00Z", aggregate.windowStart)
    assertEquals("1970-01-01T00:01:00Z", aggregate.windowEnd)
    assertEquals("1m", aggregate.windowLabel)
    assertEquals(now.toString, aggregate.ingestTs)
    assertEquals(105.0, aggregate.close)
  }
}
