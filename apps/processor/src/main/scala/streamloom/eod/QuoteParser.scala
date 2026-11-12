package streamloom.eod

import java.time.{Clock, Instant, LocalDateTime, ZoneOffset}
import java.util.Locale
import scala.util.Try
import scala.util.control.NonFatal

object QuoteParser {
  def parse(raw: String): Either[InvalidQuote, Quote] = {
    if (blank(raw)) return Left(InvalidQuote(raw, "empty_payload"))

    try {
      val quote = Json.read(raw, classOf[Quote])
      validate(quote) match {
        case Some(error) => Left(InvalidQuote(raw, error))
        case None => Right(quote)
      }
    } catch {
      case NonFatal(error) => Left(InvalidQuote(raw, s"parse_error: ${error.getMessage}"))
    }
  }

  def validate(quote: Quote): Option[String] = {
    if (quote == null) Some("null_quote")
    else if (blank(quote.symbol)) Some("missing_symbol")
    else if (blank(quote.date) || blank(quote.time)) Some("missing_datetime")
    else if (Seq(quote.open, quote.high, quote.low, quote.close).exists(_.isNaN)) Some("invalid_ohlc")
    else if (quote.volume < 0) Some("invalid_volume")
    else None
  }

  def normalize(quote: Quote, clock: Clock = Clock.systemUTC()): Quote = {
    val eventTime = Try {
      LocalDateTime.parse(s"${quote.date}T${quote.time}").toInstant(ZoneOffset.UTC).toEpochMilli
    }.orElse(Try(Instant.parse(quote.fetchedAt).toEpochMilli)).getOrElse(0L)

    quote.copy(
      symbol = quote.symbol.toUpperCase(Locale.ROOT),
      source = if (blank(quote.source)) "stooq" else quote.source,
      eventTimeMs = if (eventTime == 0L) clock.millis() else eventTime
    )
  }

  private def blank(value: String): Boolean = value == null || value.isBlank
}
