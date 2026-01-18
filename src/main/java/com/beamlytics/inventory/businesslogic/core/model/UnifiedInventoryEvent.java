package com.beamlytics.inventory.businesslogic.core.model;

import javax.annotation.Nullable;
import org.apache.beam.sdk.schemas.AutoValueSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.apache.beam.sdk.schemas.annotations.SchemaFieldName;
import com.google.auto.value.AutoValue;

import java.io.Serializable;

@DefaultSchema(AutoValueSchema.class)
@AutoValue
public abstract class UnifiedInventoryEvent implements Serializable {

    public enum EventType {
        SALE,
        RECEIPT,
        SHIPMENT,
        RESET,
        RETURN,
        VIEW,
        CART_ADD
    }

    @SchemaFieldName("product_id")
    public abstract String getProductId();

    @SchemaFieldName("store_id")
    public abstract String getStoreId();

    @SchemaFieldName("event_type")
    public abstract EventType getEventType();

    @SchemaFieldName("quantity")
    public abstract Long getQuantity();

    @SchemaFieldName("timestamp")
    public abstract Long getTimestamp();

    @SchemaFieldName("metadata")
    public abstract @Nullable String getMetadata();

    public abstract Builder toBuilder();

    public static Builder builder() {
        return new AutoValue_UnifiedInventoryEvent.Builder();
    }

    @AutoValue.Builder
    public abstract static class Builder {
        public abstract Builder setProductId(String productId);

        public abstract Builder setStoreId(String storeId);

        public abstract Builder setEventType(EventType eventType);

        public abstract Builder setQuantity(Long quantity);

        public abstract Builder setTimestamp(Long timestamp);

        public abstract Builder setMetadata(String metadata);

        public abstract UnifiedInventoryEvent build();
    }
}
