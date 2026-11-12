package streamloom.eod

import java.time.{Clock, Instant, ZoneOffset}
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.{NullAndEmptySource, ValueSource}

class QuoteParserTest {
  private val clock = Clock.fixed(Instant.parse("2026-11-12T16:00:00Z"), ZoneOffset.UTC)

  @Test def accepteUneCotationValide(): Unit = {
    val quote = QuoteExamples.apple
    assertEquals(Right(quote), QuoteParser.parse(Json.write(quote)))
  }

  @Test def accepteLesChampsSupplementairesDuProducteur(): Unit = {
    val json = Json.write(QuoteExamples.apple).dropRight(1) + ",\"extra\":true}"
    assertEquals(Right(QuoteExamples.apple), QuoteParser.parse(json))
  }

  @ParameterizedTest
  @NullAndEmptySource
  @ValueSource(strings = Array(" ", "\n"))
  def refuseUnMessageVide(raw: String): Unit = {
    assertEquals(Left(InvalidQuote(raw, "empty_payload")), QuoteParser.parse(raw))
  }

  @Test def refuseUnJsonIllisible(): Unit = {
    val error = QuoteParser.parse("hello").swap.toOption.get
    assertEquals("hello", error.raw)
    assertTrue(error.error.startsWith("parse_error:"))
  }

  @Test def refuseUneCotationNulle(): Unit = {
    assertEquals(Left(InvalidQuote("null", "null_quote")), QuoteParser.parse("null"))
  }

  @Test def refuseUnSymboleAbsent(): Unit = {
    assertEquals(Some("missing_symbol"), QuoteParser.validate(QuoteExamples.apple.copy(symbol = " ")))
  }

  @Test def refuseUneDateOuUneHeureAbsente(): Unit = {
    assertEquals(Some("missing_datetime"), QuoteParser.validate(QuoteExamples.apple.copy(date = null)))
    assertEquals(Some("missing_datetime"), QuoteParser.validate(QuoteExamples.apple.copy(time = "")))
  }

  @Test def refuseUnPrixNaN(): Unit = {
    assertEquals(Some("invalid_ohlc"), QuoteParser.validate(QuoteExamples.apple.copy(close = Double.NaN)))
  }

  @Test def refuseUnVolumeNegatif(): Unit = {
    assertEquals(Some("invalid_volume"), QuoteParser.validate(QuoteExamples.apple.copy(volume = -1)))
  }

  @Test def normaliseLeSymboleLaSourceEtLaDateEnUtc(): Unit = {
    val original = QuoteExamples.apple.copy(source = "")
    val normalized = QuoteParser.normalize(original, clock)
    assertEquals("AAPL.US", normalized.symbol)
    assertEquals("stooq", normalized.source)
    assertEquals(Instant.parse("2026-11-12T15:30:00Z").toEpochMilli, normalized.eventTimeMs)
    assertEquals("aapl.us", original.symbol)
  }

  @Test def conserveUneSourceExplicite(): Unit = {
    val normalized = QuoteParser.normalize(QuoteExamples.apple.copy(source = "manual"), clock)
    assertEquals("manual", normalized.source)
  }

  @Test def utiliseFetchedAtSiLaDateEstIllisible(): Unit = {
    val normalized = QuoteParser.normalize(QuoteExamples.apple.copy(date = "invalide"), clock)
    assertEquals(Instant.parse("2026-11-12T15:30:05Z").toEpochMilli, normalized.eventTimeMs)
  }

  @Test def utiliseLHorlogeSiLesDeuxDatesSontIllisibles(): Unit = {
    val quote = QuoteExamples.apple.copy(date = "invalide", fetchedAt = null)
    assertEquals(clock.millis(), QuoteParser.normalize(quote, clock).eventTimeMs)
  }
}
