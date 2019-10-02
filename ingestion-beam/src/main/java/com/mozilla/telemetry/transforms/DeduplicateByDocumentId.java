/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package com.mozilla.telemetry.transforms;

import com.mozilla.telemetry.ingestion.core.Constant.Attribute;
import com.mozilla.telemetry.util.Time;
import java.time.Instant;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import org.apache.beam.sdk.io.Compression;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.options.ValueProvider.StaticValueProvider;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.FlatMapElements;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Partition;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.joda.time.Duration;

/**
 * Transform that introduces a GroupByKey operation to group together all duplicate messages in
 * the batch, based on document_id.
 *
 * <p>The intention is that the logic here should exactly match the daily copy_deduplicate
 * queries that we run to copy from _live tables to _stable tables. See
 * https://github.com/mozilla/bigquery-etl/blob/master/script/copy_deduplicate
 *
 * <p>Records without a document_id attribute bypass the GroupByKey and are not deduplicated.
 *
 * <p>We apply no windowing, so the GroupByKey will be global. This assumes the pipeline is in
 * batch mode and that the pipeline is processing a single day of data.
 */
public class DeduplicateByDocumentId
    extends PTransform<PCollection<PubsubMessage>, PCollection<PubsubMessage>> {

  public static DeduplicateByDocumentId of() {
    return INSTANCE;
  }

  @Override
  public PCollection<PubsubMessage> expand(PCollection<PubsubMessage> input) {
    // Deduplicate only within whole days.
    input = input.apply(Window.into(FixedWindows.of(Duration.standardDays(1))));

    // The great majority of documents are not duplicated, so we can avoid shuffling most records
    // by first identifying which ones are potential duplicates; we extract just document IDs here
    // and create a view of IDs that occur more than once, which we then pass as side input to
    // a function that keys records by whether or not they need to be deduplicated.
    PCollection<KV<String, Long>> duplicateIds = input
        .apply("ExtractDocumentId",
            FlatMapElements.into(TypeDescriptors.strings())
                .via(message -> Optional.ofNullable(message.getAttributeMap())
                    .map(m -> m.get(Attribute.DOCUMENT_ID)).map(Collections::singleton)
                    .orElseGet(Collections::emptySet)))
        .apply("CountPerId", Count.perElement())
        .apply("FilterOutUnduplicatedIds", Filter.by(kv -> kv.getValue() > 1));
    PCollectionView<Map<String, Long>> duplicatedIdsView = duplicateIds.apply(View.asMap());

    PCollectionList<KV<Boolean, PubsubMessage>> partitioned = input.apply("KeyByMaybeDuplicated",
        ParDo.of(new DoFn<PubsubMessage, KV<Boolean, PubsubMessage>>() {

          @ProcessElement
          public void processElement(@Element PubsubMessage message, ProcessContext c) {
            Map<String, Long> dupeIds = c.sideInput(duplicatedIdsView);
            boolean maybeDupe = Optional.ofNullable(message)
                .map(m -> m.getAttribute(Attribute.DOCUMENT_ID)).filter(dupeIds::containsKey)
                .isPresent();
            c.output(KV.of(maybeDupe, message));
          }
        }).withSideInputs(duplicatedIdsView)).apply("PartitionByMaybeDuplicated",
            Partition.of(2, (kv, numPartitions) -> kv.getKey() ? 0 : 1));

    PCollection<PubsubMessage> maybeDupes = partitioned.get(0).apply("MaybeDupeValues",
        Values.create());
    PCollection<PubsubMessage> nonDupes = partitioned.get(1).apply("NonDupeValues",
        Values.create());

    PCollection<PubsubMessage> deduplicated = maybeDupes //
        // We compress payloads before grouping by key to reduce I/O impact of the GroupByKey,
        // which forces the pipeline to checkpoint state to disk.
        .apply(CompressPayload.of(StaticValueProvider.of(Compression.GZIP)))
        .apply("KeyByDocumentId",
            MapElements
                .into(TypeDescriptors.kvs(TypeDescriptors.strings(),
                    TypeDescriptor.of(PubsubMessage.class)))
                .via(message -> KV.of(message.getAttribute(Attribute.DOCUMENT_ID), message)))
        // Combine.perKey is preferable to GroupByKey + reduction, because the bifunction passed
        // to Combine.perKey can be applied in parallel to subsets of documents with the same key
        // rather than having to do a complete shuffle to colocate all documents per key.
        .apply("CombineByTakingEarliest", Combine.perKey((m1, m2) -> {
          Instant i1 = Time.parseAsInstantOrNull(m1.getAttribute(Attribute.SUBMISSION_TIMESTAMP));
          Instant i2 = Time.parseAsInstantOrNull(m2.getAttribute(Attribute.SUBMISSION_TIMESTAMP));
          if (i2 == null) {
            // If m2 has an unparseable timestamp, we consider m1 the earlier record.
            return m1;
          } else if (i1 == null || i1.toEpochMilli() > i2.toEpochMilli()) {
            return m2;
          } else {
            return m1;
          }
        })).apply("DeduplicatedValues", Values.create())
        .apply(DecompressPayload.enabled(StaticValueProvider.of(true)));

    return PCollectionList.of(nonDupes).and(deduplicated) //
        .apply(Flatten.pCollections());
  }

  ////

  private static DeduplicateByDocumentId INSTANCE = new DeduplicateByDocumentId();

  private DeduplicateByDocumentId() {
  }
}
