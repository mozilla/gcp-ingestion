from google.cloud.pubsub_v1 import PublisherClient, types
import pytest


async def test_batch_reject_message(client: PublisherClient):
    client.batch_settings = types.BatchSettings(1, 0, 1)
    with pytest.raises(ValueError):
        client.publish("", b"..")
    for batch in client._batches.values():
        await batch.result
