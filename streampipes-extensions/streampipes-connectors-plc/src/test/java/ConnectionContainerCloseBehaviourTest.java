/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

import org.apache.streampipes.extensions.connectors.plc.cache.SpConnectionContainer;
import org.apache.streampipes.extensions.connectors.plc.cache.SpLeasedPlcConnection;

import org.apache.plc4x.java.api.PlcConnection;
import org.apache.plc4x.java.api.PlcConnectionManager;
import org.apache.plc4x.java.api.authentication.PlcAuthentication;
import org.apache.plc4x.java.api.exceptions.PlcConnectionException;
import org.apache.plc4x.java.api.messages.PlcBrowseRequest;
import org.apache.plc4x.java.api.messages.PlcPingResponse;
import org.apache.plc4x.java.api.messages.PlcReadRequest;
import org.apache.plc4x.java.api.messages.PlcSubscriptionRequest;
import org.apache.plc4x.java.api.messages.PlcUnsubscriptionRequest;
import org.apache.plc4x.java.api.messages.PlcWriteRequest;
import org.apache.plc4x.java.api.metadata.PlcConnectionMetadata;
import org.apache.plc4x.java.api.model.PlcTag;
import org.apache.plc4x.java.api.value.PlcValue;
import org.apache.plc4x.java.utils.cache.exceptions.PlcConnectionManagerClosedException;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ConnectionContainerCloseBehaviourTest {

  /** Simple manager that always returns the same dummy connection. */
  static class SimpleManager implements PlcConnectionManager {
    final DummyConnection connection = new DummyConnection();
    final AtomicInteger calls = new AtomicInteger();
    @Override public PlcConnection getConnection(String url) throws PlcConnectionException {
      calls.incrementAndGet();
      return connection;
    }

    @Override
    public PlcConnection getConnection(String s, PlcAuthentication plcAuthentication) throws PlcConnectionException {
      return null;
    }
  }

  /** Minimal stub for PlcConnection; add more methods if your PLC4X version requires them. */
  static class DummyConnection implements PlcConnection {
    final AtomicBoolean closed = new AtomicBoolean(false);
    @Override public void connect() {}
    @Override public boolean isConnected() { return !closed.get(); }
    @Override public void close() { closed.set(true); }

    @Override
    public Optional<PlcTag> parseTagAddress(String s) {
      return Optional.empty();
    }

    @Override
    public Optional<PlcValue> parseTagValue(PlcTag plcTag, Object... objects) {
      return Optional.empty();
    }

    @Override
    public PlcConnectionMetadata getMetadata() {
      return null;
    }

    @Override
    public CompletableFuture<? extends PlcPingResponse> ping() {
      return null;
    }

    @Override
    public PlcReadRequest.Builder readRequestBuilder() {
      return null;
    }

    @Override
    public PlcWriteRequest.Builder writeRequestBuilder() {
      return null;
    }

    @Override
    public PlcSubscriptionRequest.Builder subscriptionRequestBuilder() {
      return null;
    }

    @Override
    public PlcUnsubscriptionRequest.Builder unsubscriptionRequestBuilder() {
      return null;
    }

    @Override
    public PlcBrowseRequest.Builder browseRequestBuilder() {
      return null;
    }

    @Override public String toString() { return "DummyConnection"; }
    // Uncomment/implement any additional builders if your interface requires:
    // @Override public PlcReadRequest.Builder readRequestBuilder() { throw new UnsupportedOperationException(); }
    // @Override public PlcWriteRequest.Builder writeRequestBuilder() { throw new UnsupportedOperationException(); }
    // @Override public PlcSubscriptionRequest.Builder subscriptionRequestBuilder() { throw new UnsupportedOperationException(); }
    // @Override public PlcUnsubscriptionRequest.Builder unsubscriptionRequestBuilder() { throw new UnsupportedOperationException(); }
    // @Override public PlcBrowseRequest.Builder browseRequestBuilder() { throw new UnsupportedOperationException(); }
    // @Override public boolean ping() { return true; }
  }

  @Test
  void close_failsQueuedWaiters_preventsNewLeases_andClosesUnderlying() throws Exception {
    SimpleManager mgr = new SimpleManager();

    SpConnectionContainer cc = new SpConnectionContainer(
        mgr, "mock://plc",
        Duration.ofSeconds(30), Duration.ofSeconds(30),
        url -> null // closeConnectionHandler
    );

    // 1) Acquire first lease (active).
    SpLeasedPlcConnection lease1 =
        (SpLeasedPlcConnection) cc.lease().get(500, TimeUnit.MILLISECONDS);

    // 2) Enqueue a second waiter (it should NOT complete yet).
    Future<PlcConnection> queued = cc.lease();

    // 3) Close the container while one lease is active and another is queued.
    cc.close();

    // 3a) Queued waiter must fail with PlcConnectionManagerClosedException.
    ExecutionException ex =
        assertThrows(ExecutionException.class, () -> queued.get(300, TimeUnit.MILLISECONDS));
    assertTrue(ex.getCause() instanceof PlcConnectionManagerClosedException,
        "Queued waiter should fail with PlcConnectionManagerClosedException");

    // 3b) Underlying connection must be closed.
    assertTrue(mgr.connection.closed.get(), "Underlying PlcConnection.close() should have been called");

    // 3c) New lease attempts must fail fast with PlcConnectionManagerClosedException.
    Future<PlcConnection> afterClose = cc.lease();
    ExecutionException ex2 =
        assertThrows(ExecutionException.class, () -> afterClose.get(300, TimeUnit.MILLISECONDS));
    assertTrue(ex2.getCause() instanceof PlcConnectionManagerClosedException);

    // 3d) Returning the old lease after close should not resurrect anything.
    //     (Should be a no-op and not throw.)
    cc.returnConnection(lease1, /*invalidate*/ false);

    // 3e) Still cannot lease after returning.
    Future<PlcConnection> stillClosed = cc.lease();
    ExecutionException ex3 =
        assertThrows(ExecutionException.class, () -> stillClosed.get(300, TimeUnit.MILLISECONDS));
    assertTrue(ex3.getCause() instanceof PlcConnectionManagerClosedException);
  }

  @Test
  void close_cancelsIdleTimer_andDoesNotInvokeCloseHandler() throws Exception {
    SimpleManager mgr = new SimpleManager();
    AtomicInteger closeHandlerCalls = new AtomicInteger(0);

    // Use a very short idle time so the timer would fire quickly if not cancelled.
    Duration maxIdle = Duration.ofMillis(150);

    SpConnectionContainer cc = new SpConnectionContainer(
        mgr, "mock://plc",
        Duration.ofSeconds(5), maxIdle,
        url -> { closeHandlerCalls.incrementAndGet(); return null; }
    );

    // 1) Acquire and then return a lease with an empty queue -> this arms the idle timer.
    SpLeasedPlcConnection lease =
        (SpLeasedPlcConnection) cc.lease().get(500, TimeUnit.MILLISECONDS);
    cc.returnConnection(lease, /*invalidate*/ false); // with empty queue, this starts idle timer

    // 2) Immediately close the container; this should cancel the idle timer.
    cc.close();

    // 3) Wait longer than the idle timeout; the handler must NOT be invoked.
    Thread.sleep(maxIdle.toMillis() + 200);
    assertEquals(0, closeHandlerCalls.get(), "closeConnectionHandler should not be invoked after close() cancels the timer");

    // Also ensure new leases are rejected after close().
    Future<PlcConnection> f = cc.lease();
    ExecutionException ex = assertThrows(ExecutionException.class, () -> f.get(300, TimeUnit.MILLISECONDS));
    assertTrue(ex.getCause() instanceof PlcConnectionManagerClosedException);
  }
}
