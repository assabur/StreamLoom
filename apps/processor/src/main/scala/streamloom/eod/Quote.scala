package streamloom.eod

import com.fasterxml.jackson.annotation.JsonIgnoreProperties

@JsonIgnoreProperties(ignoreUnknown = true)
final case class Quote(
    symbol: String = null,
    date: String = null,
    time: String = null,
    open: Double = 0,
    high: Double = 0,
    low: Double = 0,
    close: Double = 0,
    volume: Long = 0,
    source: String = null,
    fetchedAt: String = null,
    eventTimeMs: Long = 0
)

final case class InvalidQuote(raw: String, error: String)
final case class DltRecord(raw: String, error: String, ingestTs: String)
