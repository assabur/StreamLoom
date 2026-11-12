package streamloom.eod

import com.fasterxml.jackson.annotation.JsonInclude
import com.fasterxml.jackson.databind.{ObjectMapper, PropertyNamingStrategies}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.kafka.common.serialization.{Deserializer, Serde, Serializer}

object Json {
  private val mapper = new ObjectMapper()
    .registerModule(DefaultScalaModule)
    .setPropertyNamingStrategy(PropertyNamingStrategies.SNAKE_CASE)
    .setSerializationInclusion(JsonInclude.Include.NON_NULL)

  def write(value: AnyRef): String = mapper.writeValueAsString(value)
  def read[T](value: String, valueType: Class[T]): T = mapper.readValue(value, valueType)
}

final class JsonSerde[T <: AnyRef](valueType: Class[T]) extends Serde[T] {
  override def serializer(): Serializer[T] = (_: String, value: T) =>
    if (value == null) null else Json.write(value).getBytes(java.nio.charset.StandardCharsets.UTF_8)

  override def deserializer(): Deserializer[T] = (_: String, bytes: Array[Byte]) =>
    if (bytes == null) null.asInstanceOf[T]
    else Json.read(new String(bytes, java.nio.charset.StandardCharsets.UTF_8), valueType)
}
