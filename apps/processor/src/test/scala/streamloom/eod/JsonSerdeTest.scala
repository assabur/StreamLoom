package streamloom.eod

import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test

class JsonSerdeTest {
  @Test def relitUneCotationSansPerte(): Unit = {
    val serde = new JsonSerde(classOf[Quote])
    val quote = QuoteExamples.apple.copy(source = "entrée manuelle")
    val bytes = serde.serializer().serialize("eod", quote)
    assertEquals(quote, serde.deserializer().deserialize("eod", bytes))
  }

  @Test def relitLEtatDesAgregatsSansPerte(): Unit = {
    val serde = new JsonSerde(classOf[OhlcvState])
    val state = OhlcvState().update(QuoteExamples.apple.copy(eventTimeMs = 1234))
    val bytes = serde.serializer().serialize("state", state)
    assertEquals(state, serde.deserializer().deserialize("state", bytes))
  }

  @Test def conserveLesTombstonesKafka(): Unit = {
    val serde = new JsonSerde(classOf[Quote])
    assertNull(serde.serializer().serialize("eod", null))
    assertNull(serde.deserializer().deserialize("eod", null))
  }

  @Test def produitLesNomsJsonAttendusParClickHouse(): Unit = {
    val json = Json.write(QuoteExamples.apple.copy(eventTimeMs = 1234))
    assertTrue(json.contains("\"fetched_at\""))
    assertTrue(json.contains("\"event_time_ms\":1234"))
    assertFalse(json.contains("eventTimeMs"))
    val dlt = Json.write(DltRecord("hello", "parse_error", "2026-11-12T16:00:00Z"))
    assertTrue(dlt.contains("\"ingest_ts\""))
  }

  @Test def refuseUnEtatCorrompu(): Unit = {
    val serde = new JsonSerde(classOf[OhlcvState])
    assertThrows(classOf[com.fasterxml.jackson.core.JsonProcessingException], () => {
      serde.deserializer().deserialize("state", "not-json".getBytes(java.nio.charset.StandardCharsets.UTF_8))
      ()
    })
  }
}
