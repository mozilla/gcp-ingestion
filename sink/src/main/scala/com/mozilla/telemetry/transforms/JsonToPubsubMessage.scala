/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */
package com.mozilla.telemetry.transforms

import com.fasterxml.jackson.annotation.{JsonCreator, JsonProperty}
import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.beam.sdk.io.gcp.pubsub.{PubsubMessage, PubsubMessageWithAttributesCoder}
import org.apache.beam.sdk.transforms.DoFn.{Element, MultiOutputReceiver, ProcessElement}
import org.apache.beam.sdk.transforms.{DoFn, ParDo, PTransform}
import org.apache.beam.sdk.values.{PCollection, PCollectionTuple, TupleTag, TupleTagList}

object JsonToPubsubMessage extends PTransform[PCollection[String],PCollectionTuple] {
  val mainTag: TupleTag[PubsubMessage] = new TupleTag()
  val errorTag: TupleTag[PubsubMessage] = new TupleTag()

  object Fn extends DoFn[String,PubsubMessage] {
    /* Required to decode PubsubMessage from json
     *
     * This is necessary because jackson can automatically determine how to encode
     * PubsubMessage as json, but it can't automatically tell how to decode it
     * because the 'getAttributeMap' method returns the value for the 'attributes'
     * parameter. Additionally jackson doesn't like that there are no setter
     * methods on PubsubMessage.
     *
     * The default jackson output format for PubsubMessage, which we want to read,
     * looks like:
     * {
     *   "payload": "<base64 encoded byte array",
     *   "attributeMap": {"<key>": "<value>"...}
     * }
     */
    @JsonCreator
    abstract class PubsubMessageMixin(
      @JsonProperty("payload") val payload: Array[Byte],
      @JsonProperty("attributeMap") val attributes: java.util.Map[String,String]) {
    }

    val objectMapper = new ObjectMapper()
      .addMixIn(classOf[PubsubMessage], classOf[PubsubMessageMixin])

    def transform(element: String): PubsubMessage = objectMapper.readValue(
      element, classOf[PubsubMessage]
    ) match {
      case null => throw new java.lang.NullPointerException
      case msg if msg.getPayload == null => throw new java.lang.NullPointerException
      case msg => msg
    }

    def toError(element: String, error: Throwable): PubsubMessage = {
      val attributes = new java.util.HashMap[String,String]()
      // put required error fields
      attributes.put("error_type", this.toString)
      attributes.put("error_message", error.toString)
      // return a new PubsubMessage
      new PubsubMessage(element.getBytes, attributes)
    }

    @ProcessElement
    def processElement(@Element element: String, out: MultiOutputReceiver): Unit = {
      try {
        transform(element)
      } catch {
        case error: Throwable => out.get(errorTag).output(toError(element, error))
      }
    }
  }

  override def expand(input: PCollection[String]): PCollectionTuple = {
    val output = input
      .apply(ParDo
        .of(Fn)
        .withOutputTags(mainTag, TupleTagList.of(errorTag)))
    output.get(mainTag).setCoder(PubsubMessageWithAttributesCoder.of)
    output.get(errorTag).setCoder(PubsubMessageWithAttributesCoder.of)
    output
  }
}
