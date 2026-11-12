package streamloom.eod

object QuoteExamples {
  def apple: Quote = Quote(
    symbol = "aapl.us", date = "2026-11-12", time = "15:30:00",
    open = 100, high = 110, low = 90, close = 105, volume = 10,
    source = "stooq", fetchedAt = "2026-11-12T15:30:05Z"
  )
}
