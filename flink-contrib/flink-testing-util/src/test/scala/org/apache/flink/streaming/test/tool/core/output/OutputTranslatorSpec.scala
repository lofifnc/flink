package org.apache.flink.streaming.test.tool.core.output

import org.apache.flink.streaming.test.tool.CoreSpec
import org.apache.flink.streaming.test.tool.output.OutputVerifier
import org.mockito.Mockito._

class StringToInt(verifier: OutputVerifier[Int])
  extends OutputTranslator[String,Int](verifier) {
  override def translate(record: String): Int = Integer.parseInt(record)
}

class OutputTranslatorSpec extends CoreSpec {

  "the translator" should "translate output" in {
    val verifier = mock[OutputVerifier[Int]]
    val translatedVerifier = new StringToInt(verifier)

    translatedVerifier.init()
    translatedVerifier.receive("1")
    translatedVerifier.receive("2")
    translatedVerifier.receive("3")
    translatedVerifier.receive("4")
    translatedVerifier.receive("5")
    translatedVerifier.finish()

    verify(verifier).init()
    verify(verifier).receive(1)
    verify(verifier).receive(2)
    verify(verifier).receive(3)
    verify(verifier).receive(4)
    verify(verifier).receive(5)
    verify(verifier).finish()
  }

}
