/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.clients.consumer.internals;

import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class IdempotentCloserTest {

    private static final Runnable CALLBACK_NO_OP = () -> {};

    private static final Runnable CALLBACK_WITH_RUNTIME_EXCEPTION = () -> {
        throw new RuntimeException("Simulated error during callback");
    };

    /**
     * Tests basic functionality, i.e. that close <em>means</em> closed.
     */
    @Test
    public void testBasicClose() {
        IdempotentCloser ic = new IdempotentCloser();
        assertFalse(ic.isClosed());
        ic.close();
        assertTrue(ic.isClosed());
    }

    /**
     * Tests that the onClose callback is only invoked once.
     */
    @Test
    public void testCountCloses() {
        AtomicInteger onCloseCounter = new AtomicInteger();
        IdempotentCloser ic = new IdempotentCloser();

        // Verify initial invariants.
        assertFalse(ic.isClosed());
        assertEquals(0, onCloseCounter.get());

        // Close with our onClose callback to increment our counter.
        ic.close(onCloseCounter::getAndIncrement);
        assertTrue(ic.isClosed());
        assertEquals(1, onCloseCounter.get());

        // Close with our onClose callback again, but verify it wasn't invoked as it was previously closed.
        ic.close(onCloseCounter::getAndIncrement);
        assertTrue(ic.isClosed());
        assertEquals(1, onCloseCounter.get());
    }

    /**
     * Tests that the onClose callback is only invoked once, while the onPreviousClose callback can be invoked
     * a variable number of times.
     */
    @Test
    public void testEnsureIdempotentClose() {
        AtomicInteger onCloseCounter = new AtomicInteger();
        AtomicInteger onPreviousCloseCounter = new AtomicInteger();

        IdempotentCloser ic = new IdempotentCloser();

        // Verify initial invariants.
        assertFalse(ic.isClosed());
        assertEquals(0, onCloseCounter.get());
        assertEquals(0, onPreviousCloseCounter.get());

        // Our first close passes in both callbacks. As a result, our onClose callback should be run but our
        // onPreviousClose callback should not be invoked.
        ic.close(onCloseCounter::getAndIncrement, onPreviousCloseCounter::getAndIncrement);
        assertTrue(ic.isClosed());
        assertEquals(1, onCloseCounter.get());
        assertEquals(0, onPreviousCloseCounter.get());

        // Our second close again passes in both callbacks. As this is the second close, our onClose callback
        // should not be run but our onPreviousClose callback should be executed.
        ic.close(onCloseCounter::getAndIncrement, onPreviousCloseCounter::getAndIncrement);
        assertTrue(ic.isClosed());
        assertEquals(1, onCloseCounter.get());
        assertEquals(1, onPreviousCloseCounter.get());

        // Our third close yet again passes in both callbacks. As before, our onClose callback should not be run
        // but our onPreviousClose callback should be run again.
        ic.close(onCloseCounter::getAndIncrement, onPreviousCloseCounter::getAndIncrement);
        assertTrue(ic.isClosed());
        assertEquals(1, onCloseCounter.get());
        assertEquals(2, onPreviousCloseCounter.get());
    }

    /**
     * Tests that the {@link IdempotentCloser#maybeThrowIllegalStateException(String)} method will not throw an
     * exception if the closer is in the "open" state, but if invoked after it's in the "closed" state, it will
     * throw the exception.
     */
    @Test
    public void testCloseBeforeThrows() {
        IdempotentCloser ic = new IdempotentCloser();

        // Verify initial invariants.
        assertFalse(ic.isClosed());

        // maybeThrowIllegalStateException doesn't throw anything since the closer is still in its "open" state.
        assertDoesNotThrow(() -> ic.maybeThrowIllegalStateException(() -> "test"));

        // Post-close, our call to maybeThrowIllegalStateException will, in fact, throw said exception.
        ic.close();
        assertTrue(ic.isClosed());
        assertThrows(IllegalStateException.class, () -> ic.maybeThrowIllegalStateException(() -> "test"));
    }

    /**
     * Tests that if the onClose callback is
     */
    @Test
    public void testErrorsInOnCloseCallbacksAreNotSwallowed() {
        IdempotentCloser ic = new IdempotentCloser();
        assertFalse(ic.isClosed());
        assertThrows(RuntimeException.class, () -> ic.close(CALLBACK_WITH_RUNTIME_EXCEPTION));

        // Make sure we're still closed, though...
        assertTrue(ic.isClosed());
    }

    @Test
    public void testErrorsInOnPreviousCloseCallbacksAreNotSwallowed() {
        IdempotentCloser ic = new IdempotentCloser();
        assertFalse(ic.isClosed());
        ic.close(CALLBACK_NO_OP);
        assertThrows(RuntimeException.class, () -> ic.close(CALLBACK_NO_OP, CALLBACK_WITH_RUNTIME_EXCEPTION));
        assertTrue(ic.isClosed());
    }

    @Test
    public void testAutoCloseable() {
        try (IdempotentCloser ic = new IdempotentCloser()) {
            assertFalse(ic.isClosed());
        }
    }

    @Test
    public void testCreatedClosed() {
        IdempotentCloser ic = new IdempotentCloser(true);
        assertTrue(ic.isClosed());
        assertThrows(IllegalStateException.class, () -> ic.maybeThrowIllegalStateException(() -> "test"));
        assertDoesNotThrow(() -> ic.close(CALLBACK_WITH_RUNTIME_EXCEPTION));
    }
}
