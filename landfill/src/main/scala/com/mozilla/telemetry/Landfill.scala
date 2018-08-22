/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */
package com.mozilla.telemetry

import com.fasterxml.jackson.annotation.{JsonCreator, JsonProperty}
import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.beam.sdk.Pipeline
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessageWithAttributesCoder
import org.apache.beam.sdk.extensions.jackson.{AsJsons, ParseJsons}
import org.apache.beam.sdk.io.TextIO
import org.apache.beam.sdk.io.gcp.pubsub.{PubsubIO, PubsubMessage}
import org.apache.beam.sdk.options.Validation.Required
import org.apache.beam.sdk.options.{Default, Description, PipelineOptions, PipelineOptionsFactory, ValueProvider}
import org.apache.beam.sdk.transforms.DoFn
import org.apache.beam.sdk.transforms.DoFn.{Element, ProcessElement}
import org.apache.beam.sdk.transforms.ParDo
import org.apache.beam.sdk.transforms.windowing.{FixedWindows, Window}
import org.apache.beam.sdk.values.PCollection
import org.joda.time.Duration // scalastyle:ignore

object Landfill {
  trait Options extends PipelineOptions {
    @Description("Type of --input, must be pubsub or file")
    @Default.String("pubsub")
    def getInputType: String
    def setInputType(path: String)

    @Description("Type of --input, must be pubsub, file or stdout")
    @Default.String("file")
    def getOutputType: String
    def setOutputType(path: String)

    @Description("Fixed window duration, in minutes")
    @Default.Long(10)
    def getWindowMinutes: Long
    def setWindowMinutes(value: Long): Unit

    /* Note: Dataflow templates accept ValueProvider options at runtime, and
     * other options at creation time. When running without templates specify
     * all options at once.
     */

    @Description("Input to read from")
    @Required
    def getInput: ValueProvider[String]
    def setInput(path: ValueProvider[String])

    @Description("Output to write to")
    @Required
    def getOutput: ValueProvider[String]
    def setOutput(path: ValueProvider[String])
  }

  def main(args: Array[String]): Unit = {
    // register options class so that `--help=Options` works
    PipelineOptionsFactory.register(classOf[Options])

    val options = PipelineOptionsFactory
      .fromArgs(args: _*)
      .withValidation()
      .as(classOf[Options])

    val pipeline = Pipeline.create(options)
    val records = readInput(pipeline, options)
    writeOutput(records, options)

    pipeline.run()
  }

  def readInput(pipeline: Pipeline, options: Options): PCollection[PubsubMessage] = {
    options.getInputType.toLowerCase match {
      case "pubsub" => pipeline
        .apply(PubsubIO.readMessagesWithAttributes().fromSubscription(options.getInput))
      case "file" => pipeline
        .apply(TextIO.read.from(options.getInput))
        .apply(ParseJsons
          .of(classOf[PubsubMessage])
          .withMapper(new ObjectMapper()
            .addMixIn(classOf[PubsubMessage], classOf[PubsubMessageMixin])))
        .setCoder(PubsubMessageWithAttributesCoder.of)
    }
  }

  def writeOutput(records: PCollection[PubsubMessage], options: Options): Unit = {
    options.getOutputType.toLowerCase match {
      case "pubsub" => records
        .apply(PubsubIO.writeMessages().to(options.getOutput))
      case "stdout" => records
        .apply(AsJsons.of(classOf[PubsubMessage]))
        .apply(ParDo.of(PrintString))
      case "file" => records
        .apply(AsJsons.of(classOf[PubsubMessage]))
        .apply(Window.into(FixedWindows.of(Duration.standardMinutes(options.getWindowMinutes))))
        .apply(TextIO
          .write
          .to(options.getOutput)
          .withWindowedWrites())
    }
  }
}

/* Required to deserialize PubsubMessage from json
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

object PrintString extends DoFn[String,String] {
  @ProcessElement
  def processElement(@Element element: String): Unit = {
    println(element) // scalastyle:ignore
  }
}
