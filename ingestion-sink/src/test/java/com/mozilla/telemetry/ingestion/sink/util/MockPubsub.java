package com.mozilla.telemetry.ingestion.sink.util;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutures;
import com.google.api.gax.rpc.ApiCallContext;
import com.google.api.gax.rpc.UnaryCallable;
import com.google.cloud.pubsub.v1.stub.SubscriberStub;
import com.google.protobuf.Empty;
import com.google.pubsub.v1.AcknowledgeRequest;
import com.google.pubsub.v1.GetSubscriptionRequest;
import com.google.pubsub.v1.ModifyAckDeadlineRequest;
import com.google.pubsub.v1.PubsubMessage;
import com.google.pubsub.v1.PullRequest;
import com.google.pubsub.v1.PullResponse;
import com.google.pubsub.v1.ReceivedMessage;
import com.google.pubsub.v1.Subscription;
import java.util.LinkedList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Queue;

public class MockPubsub {

  public SubscriberStub client = mock(SubscriberStub.class);
  public MockCallable<ModifyAckDeadlineRequest, Empty> modifyAckDeadlineCallable = //
      new MockCallable<>();
  public MockCallable<AcknowledgeRequest, Empty> acknowledgeCallable = new MockCallable<>();
  public MockCallable<GetSubscriptionRequest, Subscription> getSubscriptionCallable = //
      new MockCallable<>();
  public MockCallable<PullRequest, PullResponse> pullCallable = new MockCallable<>();

  /** Constructor. */
  public MockPubsub() {
    when(client.modifyAckDeadlineCallable()).thenReturn(modifyAckDeadlineCallable);
    when(client.acknowledgeCallable()).thenReturn(acknowledgeCallable);
    when(client.getSubscriptionCallable()).thenReturn(getSubscriptionCallable);
    when(client.pullCallable()).thenReturn(pullCallable);
  }

  public MockPubsub withAcknowledge() {
    acknowledgeCallable.responses.add(Empty.getDefaultInstance());
    return this;
  }

  public MockPubsub withModifyAckDeadline() {
    modifyAckDeadlineCallable.responses.add(Empty.getDefaultInstance());
    return this;
  }

  public MockPubsub withSubscription(Subscription response) {
    getSubscriptionCallable.responses.add(response);
    return this;
  }

  public MockPubsub withSubscription(String subscription, int ackDeadlineSeconds) {
    return withSubscription(Subscription.newBuilder().setName(subscription)
        .setAckDeadlineSeconds(ackDeadlineSeconds).build());
  }

  public MockPubsub withPullResponse(PullResponse response) {
    pullCallable.responses.add(response);
    return this;
  }

  public MockPubsub withPullResponse(PubsubMessage message, String ackId) {
    return withPullResponse(PullResponse.newBuilder().addReceivedMessages(
        ReceivedMessage.newBuilder().setAckId(ackId).setMessage(message).build()).build());
  }

  public static class MockCallable<RequestT, ResponseT> extends UnaryCallable<RequestT, ResponseT> {

    public List<ApiCallContext> contexts = new LinkedList<>();
    public List<RequestT> requests = new LinkedList<>();
    public Queue<ResponseT> responses = new LinkedList<>();

    private MockCallable() {
    }

    @Override
    public ApiFuture<ResponseT> futureCall(RequestT request, ApiCallContext context) {
      this.requests.add(request);
      this.contexts.add(context);
      try {
        return ApiFutures.immediateFuture(responses.remove());
      } catch (NoSuchElementException e) {
        throw new NoSuchElementException(String.format("not enough responses for %s #%d",
            request.getClass().getSimpleName(), requests.size()));
      }
    }
  }
}
