/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */
package com.mozilla.telemetry.transforms

import com.fasterxml.jackson.databind.ObjectMapper
import com.google.api.services.bigquery.model.TableRow
import org.apache.beam.sdk.io.gcp.pubsub.{PubsubMessage, PubsubMessageWithAttributesCoder}
import org.apache.beam.sdk.transforms.DoFn.{Element, MultiOutputReceiver, ProcessElement}
import org.apache.beam.sdk.transforms.{DoFn, PTransform, ParDo}
import org.apache.beam.sdk.values.{PCollection, PCollectionTuple, TupleTag, TupleTagList}

object PubsubMessageToTableRow extends PTransform[PCollection[PubsubMessage],PCollectionTuple] {
  val mainTag: TupleTag[TableRow] = new TupleTag()
  val errorTag: TupleTag[PubsubMessage] = new TupleTag()

  object Fn extends DoFn[PubsubMessage,TableRow] {
    val objectMapper = new ObjectMapper()

    def transform(element: PubsubMessage): TableRow = objectMapper.readValue(
      new String(element.getPayload),
      classOf[TableRow]
    )

    def toError(element: PubsubMessage, error: Throwable): PubsubMessage = {
      val attributes = new java.util.HashMap[String,String]()
      // put existing attributes, if not null
      Option(element.getAttributeMap).foreach(attributes.putAll)
      // put required error fields
      attributes.put("error_type", this.toString)
      attributes.put("error_message", error.toString)
      // return a new PubsubMessage
      new PubsubMessage(element.getPayload, attributes)
    }

    @ProcessElement
    def processElement(@Element element: PubsubMessage, out: MultiOutputReceiver): Unit = {
      try {
        transform(element)
      } catch {
        case error: Throwable => out.get(errorTag).output(toError(element, error))
      }
    }
  }

  override def expand(input: PCollection[PubsubMessage]): PCollectionTuple = {
    val output = input
      .apply(ParDo
        .of(Fn)
        .withOutputTags(mainTag, TupleTagList.of(errorTag)))
    output.get(errorTag).setCoder(PubsubMessageWithAttributesCoder.of)
    output
  }
}
