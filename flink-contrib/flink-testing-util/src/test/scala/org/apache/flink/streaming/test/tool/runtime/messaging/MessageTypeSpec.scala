package org.apache.flink.streaming.test.tool.runtime.messaging

import org.apache.flink.streaming.test.tool.CoreSpec
import MessageType._

class MessageTypeSpec extends CoreSpec {

  "getMessageType" should "classify messages" in {
    MessageType
      .getMessageType("START 1 2".getBytes) shouldBe START

    MessageType
      .getMessageType("SERfoobar".getBytes) shouldBe SER

    MessageType
      .getMessageType("ELEMMarostn".getBytes) shouldBe ELEM

    MessageType
      .getMessageType("END 1".getBytes) shouldBe END

    intercept[UnsupportedOperationException] {
    MessageType
      .getMessageType("EDN 1".getBytes)
    }
  }

  "getPayload" should "return the payload of a ELEM message" in {
    val payload = ELEM.getPayload("ELEMblabla".getBytes)
    new String(payload) shouldBe "blabla"
  }

  "getPayload" should "return the payload of a SER message" in {
    val payload = SER.getPayload("SERblabla".getBytes)
    new String(payload) shouldBe "blabla"
  }

  "isType" should "should match the type of a message" in {
    MessageType.isType("SERblabla".getBytes,SER) shouldBe true
    MessageType.isType("SERblabla".getBytes,START) shouldBe false
  }


}
