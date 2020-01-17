package com.mozilla.telemetry.heka;

import com.mozilla.telemetry.ingestion.core.Constant.Attribute;
import com.mozilla.telemetry.transforms.FailureMessage;
import com.mozilla.telemetry.transforms.WithErrors.Result;
import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.util.Optional;
import org.apache.beam.sdk.io.FileIO.ReadableFile;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessageWithAttributesCoder;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.joda.time.Instant;
import org.joda.time.format.ISODateTimeFormat;

public class HekaIO {

  public static ReadFiles readFiles() {
    return ReadFiles.INSTANCE;
  }

  public static class ReadFiles
      extends PTransform<PCollection<ReadableFile>, Result<PCollection<PubsubMessage>>> {

    TupleTag<PubsubMessage> successTag = new TupleTag<PubsubMessage>() {
    };
    TupleTag<PubsubMessage> failureTag = new TupleTag<PubsubMessage>() {
    };

    @Override
    public Result<PCollection<PubsubMessage>> expand(PCollection<ReadableFile> input) {
      PCollectionTuple tuple = input.apply("ReadHekaFile",
          ParDo.of(new Fn()).withOutputTags(successTag, TupleTagList.of(failureTag)));
      return Result.of(tuple.get(successTag).setCoder(PubsubMessageWithAttributesCoder.of()),
          tuple.get(failureTag));
    }

    private static ReadFiles INSTANCE = new ReadFiles();

    private class Fn extends DoFn<ReadableFile, PubsubMessage> {

      @ProcessElement
      public void processElement(@Element ReadableFile readableFile, MultiOutputReceiver out) {
        try (ReadableByteChannel channel = readableFile.open();
            InputStream is = Channels.newInputStream(channel)) {
          while (true) {
            try {
              PubsubMessage o = HekaReader.readHekaMessage(is);
              if (o == null) {
                break;
              }
              Instant instant = Optional.ofNullable(o.getAttribute(Attribute.SUBMISSION_TIMESTAMP))
                  .map(s -> ISODateTimeFormat.dateTimeParser().parseDateTime(s).toInstant())
                  .orElse(null);
              if (instant == null) {
                out.get(successTag).output(o);
              } else {
                out.get(successTag).outputWithTimestamp(o, instant);
              }
            } catch (Exception e) {
              // We emit one error output message per exception thrown while trying to read messages
              // out of the file; we don't have metadata about where in the file each record
              // occurs, so replaying these errors may be tricky.
              out.get(failureTag).output(FailureMessage.of(this, readableFile, e));
            }
          }
        } catch (IOException e) {
          // If we throw an exception on opening the file, we emit a single error message.
          out.get(failureTag).output(FailureMessage.of(this, readableFile, e));
        }
      }
    }
  }

}
