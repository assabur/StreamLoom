package streamloom.eod

import java.time.Instant

// Chaque mise à jour renvoie un nouvel état : aucun champ mutable à partager.
final case class OhlcvState(
    open: Double = 0,
    high: Double = 0,
    low: Double = 0,
    close: Double = 0,
    volumeSum: Long = 0,
    count: Long = 0,
    firstEventTimeMs: Long = 0,
    lastEventTimeMs: Long = 0,
    source: String = null
) {
  def update(quote: Quote): OhlcvState = {
    if (count == 0) {
      OhlcvState(quote.open, quote.high, quote.low, quote.close,
        quote.volume, 1, quote.eventTimeMs, quote.eventTimeMs, quote.source)
    } else {
      copy(
        high = math.max(high, quote.high),
        low = math.min(low, quote.low),
        close = quote.close,
        volumeSum = volumeSum + quote.volume,
        count = count + 1,
        lastEventTimeMs = quote.eventTimeMs
      )
    }
  }

  def toAggregate(symbol: String, startMs: Long, endMs: Long,
                  label: String, ingestTs: Instant): OhlcvAggregate =
    OhlcvAggregate(symbol, Instant.ofEpochMilli(startMs).toString,
      Instant.ofEpochMilli(endMs).toString, label,
      open, high, low, close, volumeSum, count, source,
      firstEventTimeMs, lastEventTimeMs, ingestTs.toString)
}

final case class OhlcvAggregate(
    symbol: String,
    windowStart: String,
    windowEnd: String,
    windowLabel: String,
    open: Double,
    high: Double,
    low: Double,
    close: Double,
    volumeSum: Long,
    count: Long,
    source: String,
    firstEventTimeMs: Long,
    lastEventTimeMs: Long,
    ingestTs: String
)
