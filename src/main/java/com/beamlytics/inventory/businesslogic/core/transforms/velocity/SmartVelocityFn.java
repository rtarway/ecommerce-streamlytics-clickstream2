package com.beamlytics.inventory.businesslogic.core.transforms.velocity;

import com.beamlytics.inventory.businesslogic.core.model.UnifiedInventoryEvent;
import org.apache.beam.sdk.state.StateSpec;
import org.apache.beam.sdk.state.StateSpecs;
import org.apache.beam.sdk.state.ValueState;
import org.apache.beam.sdk.state.MapState;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.joda.time.Instant;

import java.util.Map;

public class SmartVelocityFn extends DoFn<KV<String, UnifiedInventoryEvent>, KV<String, SmartVelocityFn.Signal>> {

    public static class Signal {
        public String type; // "FAST" or "SLOW"
        public String window; // "1h" or "24h"
        public Long count;

        public Signal(String type, String window, Long count) {
            this.type = type;
            this.window = window;
            this.count = count;
        }

        @Override
        public String toString() {
            return String.format("Signal{type='%s', window='%s', count=%d}", type, window, count);
        }
    }

    private final int fastMovingThreshold;

    public SmartVelocityFn(int fastMovingThreshold) {
        this.fastMovingThreshold = fastMovingThreshold;
    }

    // Stores the latest timestamp seen for this key to determine "Reference Time"
    @StateId("latestTimestamp")
    private final StateSpec<ValueState<Long>> latestTimestampSpec = StateSpecs.value();

    // Stores counts per minute for the last 24h (or more, managed manually)
    // Key: Minute Identifier, Value: Count
    @StateId("minuteBuckets")
    private final StateSpec<MapState<Long, Long>> minuteBucketsSpec = StateSpecs.map();

    // Stores total 24h count optimization (optional, but good for speed)
    // Let's rely on map iteration for accuracy if window is sliding

    @ProcessElement
    public void processElement(
            ProcessContext c,
            @StateId("latestTimestamp") ValueState<Long> latestTimestampState,
            @StateId("minuteBuckets") MapState<Long, Long> minuteBucketsState) {

        UnifiedInventoryEvent event = c.element().getValue();
        String key = c.element().getKey();

        // Only interested in consumption events
        if (event.getEventType() != UnifiedInventoryEvent.EventType.SALE &&
                event.getEventType() != UnifiedInventoryEvent.EventType.CART_ADD &&
                event.getEventType() != UnifiedInventoryEvent.EventType.VIEW) {
            return;
        }

        // Only count SALES for the velocity metric as per requirement?
        // User asked: "items sold in past 1 hour".
        // But also asked for urgency signal.
        // Let's assume this Fn calculates "Sales Velocity".
        // We can have separate instances or logic for Views.
        // For simplicity, let's filter for SALE here.
        if (event.getEventType() != UnifiedInventoryEvent.EventType.SALE) {
            return;
        }

        Long eventTime = event.getTimestamp();
        Long quantity = event.getQuantity(); // Sales are usually positive quantities in event? Or negative in Stock?
        // Assuming quantity is positive count of items sold.
        if (quantity < 0)
            quantity = -quantity;

        // Update Reference Time
        Long lastSeen = latestTimestampState.read();
        if (lastSeen == null || eventTime > lastSeen) {
            lastSeen = eventTime;
            latestTimestampState.write(lastSeen);
        }

        // Add to Bucket
        long eventMinute = eventTime / 60000; // Minute resolution
        Long currentBucketVal = minuteBucketsState.get(eventMinute).read();
        if (currentBucketVal == null)
            currentBucketVal = 0L;
        minuteBucketsState.put(eventMinute, currentBucketVal + quantity);

        // Calculate 1h and 24h Sums relative to Reference Time
        long oneHourAgoMinute = (lastSeen / 60000) - 60;
        long twentyFourHoursAgoMinute = (lastSeen / 60000) - (24 * 60);

        long sum1h = 0;
        long sum24h = 0;

        // Iterate all buckets?
        // For production, we should cleanup old buckets.
        // Here, we iterate and sum valid ones.
        for (Map.Entry<Long, Long> entry : minuteBucketsState.entries().read()) {
            long bucketMinute = entry.getKey();
            long count = entry.getValue();

            if (bucketMinute > oneHourAgoMinute) {
                sum1h += count;
            }
            if (bucketMinute > twentyFourHoursAgoMinute) {
                sum24h += count;
            } else {
                // Cleanup old data lazily
                // This might be slow if map grows large.
                // In prod, use Timer to clear old keys.
                minuteBucketsState.remove(bucketMinute);
            }
        }

        // Decision Logic
        if (sum1h >= fastMovingThreshold) {
            c.output(KV.of(key, new Signal("FAST", "1h", sum1h)));
        } else {
            c.output(KV.of(key, new Signal("SLOW", "24h", sum24h)));
        }
    }
}
