/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */
package com.mozilla.telemetry

import com.google.api.services.bigquery.model.TableRow
import com.mozilla.telemetry.transforms.{JsonToPubsubMessage, PubsubMessageToTableRow}
import org.apache.beam.sdk.Pipeline
import org.apache.beam.sdk.extensions.jackson.AsJsons
import org.apache.beam.sdk.io.TextIO
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO
import org.apache.beam.sdk.io.gcp.pubsub.{PubsubIO, PubsubMessage}
import org.apache.beam.sdk.options.Validation.Required
import org.apache.beam.sdk.options.{Default, Description, PipelineOptions, PipelineOptionsFactory, ValueProvider}
import org.apache.beam.sdk.transforms.{MapElements, PTransform}
import org.apache.beam.sdk.transforms.windowing.{FixedWindows, Window}
import org.apache.beam.sdk.values.{PBegin, PCollection, PCollectionTuple, PDone, TypeDescriptor, TypeDescriptors}
import org.joda.time.Duration // scalastyle:ignore

object Sink {
  trait Options extends PipelineOptions {
    @Description("Type of --input; must be one of [pubsub, file]")
    @Default.String("pubsub")
    def getInputType: String
    def setInputType(path: String)

    @Description("File format for --inputType=file; must be one of"
      + " json (each line contains payload[String] and attributeMap[String,String]) or"
      + " text (each line is payload)")
    @Default.String("json")
    def getInputFileFormat: String
    def setInputFileFormat(path: String)

    @Description("Type of --output; must be one of [pubsub, file, stdout]")
    @Default.String("file")
    def getOutputType: String
    def setOutputType(path: String)

    @Description("File format for --outputType=file|stdout; must be one of "
      + " json (each line contains payload[String] and attributeMap[String,String]) or"
      + " text (each line is payload)")
    @Default.String("json")
    def getOutputFileFormat: String
    def setOutputFileFormat(path: String)

    @Description("Type of --errorOutput; must be one of [pubsub, file]")
    @Default.String("pubsub")
    def getErrorOutputType: String
    def setErrorOutputType(path: String)

    @Description("Fixed window duration, in minutes")
    @Default.Long(10)
    def getWindowMinutes: Long
    def setWindowMinutes(value: Long): Unit

    /* Note: Dataflow templates accept ValueProvider options at runtime, and
     * other options at creation time. When running without templates specify
     * all options at once.
     */

    @Description("Input to read from (path to file, PubSub subscription, etc.)")
    @Required
    def getInput: ValueProvider[String]
    def setInput(path: ValueProvider[String])

    @Description("Output to write to (path to file or directory, Pubsub topic, etc.)")
    @Required
    def getOutput: ValueProvider[String]
    def setOutput(path: ValueProvider[String])

    @Description("Error output to write to (path to file or directory, Pubsub topic, etc.)")
    @Required
    def getErrorOutput: ValueProvider[String]
    def setErrorOutput(path: ValueProvider[String])
  }

  /**
    * A helper trait for the Scala pattern of defining ADTs
    * (algebraic data types) that serve as enumerations.
    *
    * We have several pipeline configuration options that logically should only
    * allow a small set of defined strings, and this pattern allows us to do
    * exhaustive pattern matches on case objects rather than on string literals.
    * It also allows us to centralize error handling for invalid inputs.
    *
    * For more on ADT-based enumerations, see https://stackoverflow.com/a/13347823/1260237
    *
    * @tparam T the sealed trait type for this enumeration
    */
  trait ConfigEnum[T] {
    def values: Seq[T]
    def names: Seq[String] = values.map(_.toString.toLowerCase)
    def valuesByName: Map[String, T] = (names, values).zipped.toMap
    def namesByValue: Map[T, String] = (values, names).zipped.toMap
    def apply(name: String): T = {
      valuesByName.getOrElse(name.toLowerCase(),
        throw new IllegalArgumentException(s"$name is not a valid ${this.toString};"
        + s" must be one of [${names.mkString(", ")}]"))
    }
  }

  sealed trait InputType
  object InputType extends ConfigEnum[InputType] {
    case object PubSub extends InputType
    case object File extends InputType
    val values = Seq(PubSub, File)
  }

  sealed trait OutputType
  object OutputType extends ConfigEnum[OutputType] {
    case object PubSub extends OutputType
    case object File extends OutputType
    case object Stdout extends OutputType
    case object BigQuery extends OutputType
    val values = Seq(PubSub, File, Stdout, BigQuery)
  }

  sealed trait ErrorOutputType
  object ErrorOutputType extends ConfigEnum[ErrorOutputType] {
    case object PubSub extends ErrorOutputType
    case object File extends ErrorOutputType
    case object Stderr extends ErrorOutputType
    val values = Seq(PubSub, File, Stderr)
  }

  sealed trait FileFormat
  object FileFormat extends ConfigEnum[FileFormat] {
    case object Json extends FileFormat
    case object Text extends FileFormat
    val values = Seq(Json, Text)
  }


  def main(args: Array[String]): Unit = {
    // register options class so that `--help=Options` works
    PipelineOptionsFactory.register(classOf[Options])

    val options = PipelineOptionsFactory
      .fromArgs(args: _*)
      .withValidation()
      .as(classOf[Options])

    val pipeline = Pipeline.create(options)

    val errorOutput = writeErrorOutput(options)
    pipeline
      .apply(readInput(options, errorOutput))
      .apply(writeOutput(options, errorOutput))

    pipeline.run()
  }

  type InputTransform = PTransform[PBegin,PCollection[PubsubMessage]]
  type OutputTransform = PTransform[PCollection[PubsubMessage],PDone]

  def readInput(options: Options, errorOutput: OutputTransform): InputTransform = {
    InputType(options.getInputType) match {
      case InputType.PubSub => PubsubIO
        .readMessagesWithAttributes()
        .fromSubscription(options.getInput)

      case InputType.File => new InputTransform {
        override def expand(input: PBegin): PCollection[PubsubMessage] = {
          val lines = input
            .apply(TextIO
              .read
              .from(options.getInput))

          FileFormat(options.getInputFileFormat) match {
            case FileFormat.Text => lines.apply(decodeText)
            case FileFormat.Json =>
              val result = lines.apply(decodeJson)
              result
                .get(decodeJson.errorTag)
                .apply(errorOutput)
              result.get(decodeJson.mainTag)
          }
        }
      }
    }
  }

  def writeOutput(options: Options, errorOutput: OutputTransform): OutputTransform = {
    val encode = FileFormat(options.getOutputFileFormat) match {
      case FileFormat.Json => encodeJson
      case FileFormat.Text => encodeText
    }

    OutputType(options.getOutputType.toLowerCase) match {
      case OutputType.PubSub => PubsubIO
        .writeMessages()
        .to(options.getOutput)

      case OutputType.File => new OutputTransform {
        override def expand(input: PCollection[PubsubMessage]): PDone = input
          .apply(encode)
          .apply(Window
            .into(FixedWindows
              .of(Duration
                .standardMinutes(options.getWindowMinutes))))
          .apply(TextIO
            .write
            .to(options.getOutput)
            .withWindowedWrites())
      }

      case OutputType.Stdout => new PTransform[PCollection[PubsubMessage],PDone] {
        override def expand(input: PCollection[PubsubMessage]): PDone = {
          input
            .apply(encode)
            .apply(MapElements
              .into(new TypeDescriptor[Unit]{})
              .via((element: String) => println(element))) // scalastyle:ignore
          PDone.in(input.getPipeline)
        }
      }

      case OutputType.BigQuery => new OutputTransform {
        override def expand(input: PCollection[PubsubMessage]): PDone = {
          val encodeTableRow: PTransform[PCollection[TableRow],PCollection[String]] = AsJsons.of(classOf[TableRow])
          val result: PCollectionTuple = input.apply(PubsubMessageToTableRow)

          result
            .get(PubsubMessageToTableRow.errorTag)
            .apply(errorOutput)

          result
            .get(PubsubMessageToTableRow.mainTag)
            .apply(BigQueryIO
              .write()
              .to(options.getOutput))
            .getFailedInserts
            .apply(encodeTableRow)
            .apply(decodeText) // TODO: add error_{type,message} fields
            .apply(errorOutput)
        }
      }
    }
  }

  def writeErrorOutput(options: Options): OutputTransform = {
    ErrorOutputType(options.getErrorOutputType.toLowerCase) match {
      case ErrorOutputType.PubSub => PubsubIO
        .writeMessages()
        .to(options.getErrorOutput)

      case ErrorOutputType.Stderr => new PTransform[PCollection[PubsubMessage],PDone] {
        override def expand(input: PCollection[PubsubMessage]): PDone = {
          input
            .apply(encodeJson)
            .apply(MapElements
              .into(new TypeDescriptor[Unit]{})
              .via((element: String) => System.err.println(element))) // scalastyle:ignore
          PDone.in(input.getPipeline)
        }
      }

      case ErrorOutputType.File => new OutputTransform {
        override def expand(input: PCollection[PubsubMessage]): PDone = input
          .apply(encodeJson)
          .apply(Window
            .into(FixedWindows
              .of(Duration
                .standardMinutes(options.getWindowMinutes))))
          .apply(TextIO
            .write
            .to(options.getErrorOutput)
            .withWindowedWrites())
      }
    }
  }

  val encodeText = MapElements
    .into(TypeDescriptors.strings)
    .via((record: PubsubMessage) => new String(record.getPayload))

  val decodeText = MapElements
    .into(new TypeDescriptor[PubsubMessage]{})
    .via((string: String) => new PubsubMessage(string.getBytes, null))

  val encodeJson = AsJsons.of(classOf[PubsubMessage])

  val decodeJson = JsonToPubsubMessage
}
