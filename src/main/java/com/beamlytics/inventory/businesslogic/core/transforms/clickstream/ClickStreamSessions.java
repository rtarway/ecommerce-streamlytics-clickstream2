/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.beamlytics.inventory.businesslogic.core.transforms.clickstream;

import org.apache.beam.sdk.schemas.transforms.Group;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.windowing.Sessions;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions;
import org.apache.http.annotation.Experimental;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.beamlytics.inventory.businesslogic.core.transforms.DeadLetterSink;
import com.google.auto.value.AutoValue;

/**
 * This transform creates sessions from the incoming clickstream using the cliendId and
 * SessionWindows. The output is a
 *
 * <pre>{@code
 * Field Name      Field Type
 * key              ROW{clientID:STRING}
 * value	        ITERABLE[ROW[ClickstreamEvent]]
 * }</pre>
 */
@Experimental
@AutoValue
public abstract class ClickStreamSessions extends PTransform<PCollection<Row>, PCollection<Row>> {

  @SuppressWarnings("unused")
  private static final Logger LOG = LoggerFactory.getLogger(DeadLetterSink.class);

  public abstract Duration getSessionWindowGapDuration();

  public abstract Builder toBuilder();

  public static ClickStreamSessions create(Duration sessionWindowGapDuration) {
    return builder().setSessionWindowGapDuration(sessionWindowGapDuration).build();
  }


  public static Builder builder() {
    return new AutoValue_ClickStreamSessions.Builder();
  }

  @AutoValue.Builder
  public abstract static class Builder {

    public abstract Builder setSessionWindowGapDuration(Duration newSessionWindowGapDuration);

    public abstract ClickStreamSessions build();
  }

  public ClickStreamSessions withSessionWindowGapDuration(Duration sessionWindowGapDuration) {
    return this.toBuilder().setSessionWindowGapDuration(sessionWindowGapDuration).build();
  }

  @Override
  public PCollection<Row> expand(PCollection<Row> input) {

    Preconditions.checkNotNull(
        this.getSessionWindowGapDuration(), "Must set a session gap duration.");
    /*
     * Group.byFiledNames returns a Row object in the format:
     *
     * <pre>{@code
     * Field Name	    Field Type
     * key	            ROW{clientID:STRING}
     * value	        ITERABLE[ROW[ClickstreamEvent]]
     * }</pre>
     */
    return input
        .apply(
            "ClickStreamSessionWindow",
            Window.into(Sessions.withGapDuration(getSessionWindowGapDuration())))
        .apply("GroupByClientId", Group.byFieldNames("client_id"));
  }
}
