package streamloom.eod

import java.time.{Clock, Duration}
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.common.utils.Bytes
import org.apache.kafka.streams.Topology
import org.apache.kafka.streams.kstream.{Materialized, Suppressed, TimeWindows}
import org.apache.kafka.streams.scala.ImplicitConversions._
import org.apache.kafka.streams.scala.StreamsBuilder
import org.apache.kafka.streams.scala.kstream.KStream
import org.apache.kafka.streams.scala.serialization.Serdes._
import org.apache.kafka.streams.state.WindowStore

object QuoteTopology {
  private implicit val quotes: Serde[Quote] = new JsonSerde(classOf[Quote])
  private implicit val states: Serde[OhlcvState] = new JsonSerde(classOf[OhlcvState])

  def build(config: StreamConfig = StreamConfig(), clock: Clock = Clock.systemUTC()): Topology = {
    val builder = new StreamsBuilder()
    val parsed = builder.stream[String, String](config.inputTopic)
      .mapValues(raw => QuoteParser.parse(raw))

    // Left contient l'erreur, Right contient la cotation valide.
    parsed.flatMapValues(result => result.swap.toOption.map { invalid =>
      Json.write(DltRecord(invalid.raw, invalid.error, clock.instant().toString))
    }.toList).to(config.dltTopic)

    val normalized = parsed
      .flatMapValues(result => result.toOption.map(quote => QuoteParser.normalize(quote, clock)).toList)
      .selectKey((_, quote) => quote.symbol)

    aggregate(normalized, TimeWindows.ofSizeAndGrace(Duration.ofMinutes(1), Duration.ofMinutes(1)),
      "1m", clock).to(config.output1mTopic)
    aggregate(normalized, TimeWindows.ofSizeAndGrace(Duration.ofDays(1), Duration.ofMinutes(5)),
      "1d", clock).to(config.outputDailyTopic)
    aggregate(normalized, TimeWindows.ofSizeAndGrace(Duration.ofHours(1), Duration.ofMinutes(5)),
      "1h", clock).to(config.output1hTopic)

    builder.build()
  }

  private def aggregate(input: KStream[String, Quote], windows: TimeWindows,
                        label: String, clock: Clock): KStream[String, String] = {
    input.groupByKey
      .windowedBy(windows)
      .aggregate(OhlcvState())((_, quote, state) => state.update(quote))(
        Materialized.`with`[String, OhlcvState, WindowStore[Bytes, Array[Byte]]](stringSerde, states)
      )
      .suppress(Suppressed.untilWindowCloses(Suppressed.BufferConfig.unbounded()))
      .toStream
      .map((key, state) => (key.key(), Json.write(state.toAggregate(
        key.key(), key.window().start(), key.window().end(), label, clock.instant()))))
  }
}
