package com.beamlytics.inventory.businesslogic.core.transforms.stock;

import com.beamlytics.inventory.businesslogic.core.model.UnifiedInventoryEvent;
import org.apache.beam.sdk.state.StateSpecs;
import org.apache.beam.sdk.state.ValueState;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.joda.time.Instant;

import java.util.Objects;

public class InventoryStatefulFn extends DoFn<KV<String, UnifiedInventoryEvent>, KV<String, Long>> {

    @StateId("lastResetTimestamp")
    private final StateSpec<ValueState<Long>> lastResetTimestampSpec = StateSpecs.value();

    @StateId("currentInventory")
    private final StateSpec<ValueState<Long>> currentInventorySpec = StateSpecs.value();

    @ProcessElement
    public void processElement(
            ProcessContext c,
            @StateId("lastResetTimestamp") ValueState<Long> lastResetTimestampState,
            @StateId("currentInventory") ValueState<Long> currentInventoryState) {

        UnifiedInventoryEvent event = c.element().getValue();
        String key = c.element().getKey();
        Long eventTimestamp = event.getTimestamp();
        Long quantity = event.getQuantity();

        Long lastReset = lastResetTimestampState.read();
        Long currentInv = currentInventoryState.read();

        if (currentInv == null) {
            currentInv = 0L;
        }

        if (event.getEventType() == UnifiedInventoryEvent.EventType.RESET) {
            // Processing a RESET event
            // Update the reset timestamp and current inventory
            lastResetTimestampState.write(eventTimestamp);
            currentInventoryState.write(quantity);

            // Emit the snapshot
            c.output(KV.of(key, quantity));
        } else {
            // Processing other events (SALE, RECEIPT, etc.)

            // Check for Late Data relative to Reset
            if (lastReset != null && eventTimestamp < lastReset) {
                // DROP: This event occurred before the last reset, so it's already accounted
                // for.
                return;
            }

            // Update inventory
            // Note: quantity should be negative for SALES, positive for RECEIPTS
            // Ensure upstream handles sign, or handle it here.
            // Assuming UnifiedInventoryEvent comes with signed quantity for simplicity,
            // or we check type. Let's rely on Type for sign if needed, but for now assume
            // signed quantity.
            // Wait, POJO definition has quantity as Long. Let's assume signed.
            // Actually, usually sales are positive counts, need subtraction.

            long delta = 0;
            switch (event.getEventType()) {
                case SALE:
                    delta = -quantity;
                    break;
                case RECEIPT:
                case RETURN:
                    delta = quantity;
                    break;
                case SHIPMENT:
                    delta = -quantity;
                    break;
                default:
                    // VIEW/CART don't impact inventory
                    return;
            }

            currentInv += delta;
            currentInventoryState.write(currentInv);
            c.output(KV.of(key, currentInv));
        }
    }
}
