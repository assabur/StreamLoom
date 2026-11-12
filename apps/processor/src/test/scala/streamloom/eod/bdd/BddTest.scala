package streamloom.eod.bdd

import org.junit.platform.suite.api.{ConfigurationParameter, IncludeEngines, SelectClasspathResource, Suite}

@Suite
@IncludeEngines(Array("cucumber"))
@SelectClasspathResource("features")
@ConfigurationParameter(key = "cucumber.glue", value = "streamloom.eod.bdd")
@ConfigurationParameter(key = "cucumber.plugin", value = "pretty,html:target/reports/cucumber.html")
class BddTest
