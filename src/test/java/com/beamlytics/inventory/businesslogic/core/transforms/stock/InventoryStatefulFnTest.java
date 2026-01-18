package com.beamlytics.inventory.businesslogic.core.transforms.stock;

import com.beamlytics.inventory.businesslogic.core.model.UnifiedInventoryEvent;
import com.beamlytics.inventory.businesslogic.core.model.UnifiedInventoryEvent.EventType;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.TestStream;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.apache.beam.sdk.testing.PAssert;

@RunWith(JUnit4.class)
public class InventoryStatefulFnTest {

        @Rule
        public final transient TestPipeline p = TestPipeline.create();

        @Test
        public void testResetsAndLateData() {
                String store = "S1";
                String product = "P1";
                String key = store + "-" + product;
                Instant baseTime = Instant.parse("2023-01-01T10:00:00Z");

                TestStream<KV<String, UnifiedInventoryEvent>> events = TestStream
                                .create(org.apache.beam.sdk.coders.KvCoder.of(
                                                org.apache.beam.sdk.coders.StringUtf8Coder.of(),
                                                org.apache.beam.sdk.coders.SerializableCoder
                                                                .of(UnifiedInventoryEvent.class)))
                                .advanceWatermarkTo(baseTime)
                                // 1. Initial Sale (T=10:00)
                                .addElements(KV.of(key, UnifiedInventoryEvent.builder()
                                                .setStoreId(store).setProductId(product).setEventType(EventType.SALE)
                                                .setQuantity(10L)
                                                .setTimestamp(baseTime.getMillis()).setMetadata(null).build()))

                                // 2. Receipt (T=10:05)
                                .addElements(KV.of(key, UnifiedInventoryEvent.builder()
                                                .setStoreId(store).setProductId(product).setEventType(EventType.RECEIPT)
                                                .setQuantity(100L)
                                                .setTimestamp(baseTime.plus(Duration.standardMinutes(5)).getMillis())
                                                .setMetadata(null)
                                                .build()))

                                // 3. Reset (T=10:10) -> Sets count to 50
                                .addElements(KV.of(key, UnifiedInventoryEvent.builder()
                                                .setStoreId(store).setProductId(product).setEventType(EventType.RESET)
                                                .setQuantity(50L)
                                                .setTimestamp(baseTime.plus(Duration.standardMinutes(10)).getMillis())
                                                .setMetadata(null)
                                                .build()))

                                // 4. LATE Sale (Timestamp T=10:02, but arrives now after Reset) -> Should be
                                // DROPPED
                                .addElements(KV.of(key, UnifiedInventoryEvent.builder()
                                                .setStoreId(store).setProductId(product).setEventType(EventType.SALE)
                                                .setQuantity(5L)
                                                .setTimestamp(baseTime.plus(Duration.standardMinutes(2)).getMillis())
                                                .setMetadata(null)
                                                .build()))

                                // 5. Normal Sale (Timestamp T=10:15) -> Should be Accepted
                                .addElements(KV.of(key, UnifiedInventoryEvent.builder()
                                                .setStoreId(store).setProductId(product).setEventType(EventType.SALE)
                                                .setQuantity(2L)
                                                .setTimestamp(baseTime.plus(Duration.standardMinutes(15)).getMillis())
                                                .setMetadata(null)
                                                .build()))

                                .advanceWatermarkToInfinity();

                PCollection<KV<String, Long>> results = p.apply(events)
                                .apply(ParDo.of(new InventoryStatefulFn()));

                // Expected sequence of updates:
                // 1. Sale(-10) -> Inventory: -10
                // 2. Receipt(+100) -> Inventory: 90
                // 3. Reset(50) -> Inventory: 50
                // 4. Late Sale -> DROPPED (No Output)
                // 5. Sale(-2) -> Inventory: 48

                PAssert.that(results).containsInAnyOrder(
                                KV.of(key, -10L),
                                KV.of(key, 90L),
                                KV.of(key, 50L),
                                KV.of(key, 48L));

                p.run();
        }
}
