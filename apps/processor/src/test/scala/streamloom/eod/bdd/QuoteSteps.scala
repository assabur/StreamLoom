package streamloom.eod.bdd

import io.cucumber.datatable.DataTable
import io.cucumber.java.{After, Before}
import io.cucumber.java.en.{Given, Then, When}
import org.junit.jupiter.api.Assertions._
import streamloom.eod._
import scala.jdk.CollectionConverters._

class QuoteSteps {
  private var kafka: TopologyFixture = _

  @Before def demarrerKafkaEnMemoire(): Unit = { kafka = new TopologyFixture() }
  @After def fermerKafkaEnMemoire(): Unit = { if (kafka != null) kafka.close() }

  @Given("les cotations suivantes reçues par Kafka :")
  def recevoirLesCotations(table: DataTable): Unit = {
    table.asMaps(classOf[String], classOf[String]).asScala.foreach { row =>
      val quote = QuoteExamples.apple.copy(
        symbol = row.get("symbole"), open = row.get("ouverture").toDouble,
        high = row.get("haut").toDouble, low = row.get("bas").toDouble,
        close = row.get("cloture").toDouble, volume = row.get("volume").toLong
      )
      kafka.send(Json.write(quote), row.get("seconde").toLong)
    }
  }

  @When("le temps du flux dépasse la fenêtre minute et sa grâce")
  def fermerLaMinute(): Unit = kafka.advanceTo(121)

  @When("le temps du flux dépasse la fenêtre horaire et sa grâce")
  def fermerLHeure(): Unit = kafka.advanceTo(3901)

  @When("le temps du flux dépasse la fenêtre journalière et sa grâce")
  def fermerLaJournee(): Unit = kafka.advanceTo(86701)

  @Then("aucun agrégat ne doit encore être publié")
  def verifierLAttente(): Unit = {
    assertTrue(kafka.minuteIsEmpty)
    assertTrue(kafka.dailyIsEmpty)
    assertTrue(kafka.hourlyIsEmpty)
  }

  @Then("la bougie {string} pour {string} contient :")
  def verifierLaBougie(label: String, symbol: String, table: DataTable): Unit = {
    val records = label match {
      case "1m" => kafka.readMinutes()
      case "1h" => kafka.readHours()
      case "1d" => kafka.readDays()
      case other => throw new IllegalArgumentException(s"Fenêtre inconnue : $other")
    }
    val matching = records.filter(_._1 == symbol)
    assertEquals(1, matching.size)
    val aggregate = matching.head._2
    val expected = table.asMaps(classOf[String], classOf[String]).get(0)
    assertEquals(symbol, aggregate.symbol)
    assertEquals(label, aggregate.windowLabel)
    assertEquals(expected.get("ouverture").toDouble, aggregate.open)
    assertEquals(expected.get("haut").toDouble, aggregate.high)
    assertEquals(expected.get("bas").toDouble, aggregate.low)
    assertEquals(expected.get("cloture").toDouble, aggregate.close)
    assertEquals(expected.get("volume").toLong, aggregate.volumeSum)
    assertEquals(expected.get("nombre").toLong, aggregate.count)
    assertTrue(kafka.dltIsEmpty)
  }

  @Then("les bougies minute sont séparées par symbole")
  def verifierLaSeparation(): Unit = {
    val records = kafka.readMinutes()
    assertEquals(Set("AAPL.US", "MSFT.US"), records.map(_._1).toSet)
    assertEquals(2, records.size)
    records.foreach { case (_, aggregate) => assertEquals(1L, aggregate.count) }
  }

  @When("je publie le message {string}")
  def publierUnTexte(raw: String): Unit = kafka.send(raw)

  @When("je publie une cotation avec un volume négatif")
  def publierUnVolumeInvalide(): Unit = kafka.send(Json.write(QuoteExamples.apple.copy(volume = -1)))

  @Then("la DLT contient le message original {string} et une erreur de parsing")
  def verifierLErreurDeParsing(raw: String): Unit = {
    val error = kafka.readError()
    assertEquals(raw, error.raw)
    assertTrue(error.error.startsWith("parse_error:"))
    assertEquals(kafka.start.toString, error.ingestTs)
  }

  @Then("la DLT contient l'erreur {string}")
  def verifierLErreurAttendue(expected: String): Unit = {
    val error = kafka.readError()
    assertEquals(expected, error.error)
    assertEquals(Json.write(QuoteExamples.apple.copy(volume = -1)), error.raw)
  }
}
