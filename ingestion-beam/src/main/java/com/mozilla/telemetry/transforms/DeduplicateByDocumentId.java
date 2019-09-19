/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package com.mozilla.telemetry.transforms;

import com.mozilla.telemetry.ingestion.core.Constant.Attribute;
import com.mozilla.telemetry.util.Time;
import java.time.Instant;
import java.util.stream.StreamSupport;
import org.apache.beam.sdk.io.Compression;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.options.ValueProvider.StaticValueProvider;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.Partition;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;

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
    PCollectionList<PubsubMessage> partitioned = input.apply("PartitionByDocumentIdPresence",
        Partition.of(2,
            (message, numPartitions) -> message.getAttributeMap().containsKey(Attribute.DOCUMENT_ID)
                ? 0
                : 1));
    PCollection<PubsubMessage> messagesWithDocId = partitioned.get(0);
    PCollection<PubsubMessage> messagesWithoutDocId = partitioned.get(1);

    PCollection<PubsubMessage> deduplicated = messagesWithDocId //
        // We compress payloads before grouping by key to reduce I/O impact of the GroupByKey,
        // which forces the pipeline to checkpoint state to disk.
        .apply(CompressPayload.of(StaticValueProvider.of(Compression.GZIP)))
        .apply("KeyByDocumentId",
            MapElements
                .into(TypeDescriptors.kvs(TypeDescriptors.strings(),
                    TypeDescriptor.of(PubsubMessage.class)))
                .via(message -> KV.of(message.getAttribute(Attribute.DOCUMENT_ID), message)))
        .apply("GroupByDocumentId", GroupByKey.create()) //
        .apply("TakeEarliestRecordPerDocumentId",
            MapElements.into(TypeDescriptor.of(PubsubMessage.class)).via(
                kv -> StreamSupport.stream(kv.getValue().spliterator(), false).min((m1, m2) -> {
                  Instant i1 = Time
                      .parseAsInstantOrNull(m1.getAttribute(Attribute.SUBMISSION_TIMESTAMP));
                  Instant i2 = Time
                      .parseAsInstantOrNull(m2.getAttribute(Attribute.SUBMISSION_TIMESTAMP));
                  if (i2 == null) {
                    // If i2 has an unparseable timestamp, we consider i1 the earlier record.
                    return -1;
                  } else if (i1 == null) {
                    return 1;
                  } else {
                    return Math.toIntExact(i1.toEpochMilli() - i2.toEpochMilli());
                  }
                }).get()))
        .apply(DecompressPayload.enabled(StaticValueProvider.of(true)));

    return PCollectionList.of(messagesWithoutDocId).and(deduplicated) //
        .apply(Flatten.pCollections());
  }

  ////

  private static DeduplicateByDocumentId INSTANCE = new DeduplicateByDocumentId();

  private DeduplicateByDocumentId() {
  }
}
