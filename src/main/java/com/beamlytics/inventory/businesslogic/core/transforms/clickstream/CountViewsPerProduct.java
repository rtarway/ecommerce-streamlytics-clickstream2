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

import com.beamlytics.inventory.dataobjects.ClickStream.PageViewAggregator;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.Field;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.schemas.transforms.*;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.joda.time.Duration;
import org.joda.time.Instant;

import javax.annotation.Nullable;

/**
 * This transform counts views per product making use of Group function using Beam Schemas. Once the
 * anlaytics are done, the results are stored in an external system. The timestamp of the
 * aggregation
 */
// @Experimental
public class CountViewsPerProduct
    extends PTransform<PCollection<Row>, PCollection<PageViewAggregator>> {

  Duration pageViewCountWindowDuration;

  public CountViewsPerProduct(Duration pageViewCountWindowDuration) {
    this.pageViewCountWindowDuration = pageViewCountWindowDuration;
  }

  public CountViewsPerProduct(@Nullable String name, Duration pageViewCountWindowDuration) {
    super(name);
    this.pageViewCountWindowDuration = pageViewCountWindowDuration;
  }

  @Override
  public PCollection<PageViewAggregator> expand(PCollection<Row> input) {

    return input
        // Remove all events but browse events.
        .apply(Filter.<Row>create().whereFieldName("event", c -> c.equals("browse")))
        // Group By pageRef and count the results.
        .apply(Window.into(FixedWindows.of(pageViewCountWindowDuration)))
        .apply(Group.<Row>byFieldNames("page").aggregateField("page", Count.combineFn(), "count"))
        .apply(CreatePageViewAggregatorMetadata.create(pageViewCountWindowDuration.getMillis()));
  }

  public static class CreatePageViewAggregatorMetadata
      extends PTransform<PCollection<Row>, PCollection<PageViewAggregator>> {

    Long durationMS;

    public static CreatePageViewAggregatorMetadata create(Long durationMS) {
      return new CreatePageViewAggregatorMetadata(durationMS);
    }

    public CreatePageViewAggregatorMetadata(Long durationMS) {
      this.durationMS = durationMS;
    }

    public CreatePageViewAggregatorMetadata(@Nullable String name, Long durationMS) {
      super(name);
      this.durationMS = durationMS;
    }

    @Override
    public PCollection<PageViewAggregator> expand(PCollection<Row> input) {

      // TODO #18 the schema registry for PageViewAggregator throws a class cast issue
      Schema schema =
          Schema.of(
              Field.of("page", FieldType.STRING),
              Field.of("count", FieldType.INT64),
              Field.of("startTime", FieldType.INT64),
              Field.of("durationMS", FieldType.INT64));

      return input
          // Note key and value are results of Group + Count operation in the previous transform.
          .apply(Select.fieldNames("key.page", "value.count"))
          // We need to add these fields to the ROW object before we convert the POJO
          .apply(
              AddFields.<Row>create()
                  .field("startTime", FieldType.INT64)
                  .field("durationMS", FieldType.INT64))
          .apply(
              ParDo.of(
                  new DoFn<Row, Row>() {
                    @ProcessElement
                    public void process(
                        @Element Row input, @Timestamp Instant time, OutputReceiver<Row> o) {
                      // The default timestamp attached to a combined value is the end of the window
                      // To find the start of the window we deduct the duration + 1 as beam windows
                      // are (start,end] with epsilon of 1 ms
                      Row row =
                          Row.fromRow(input)
                              .withFieldValue("durationMS", durationMS)
                              .withFieldValue("startTime", time.getMillis() - durationMS + 1)
                              .build();
                      o.output(row);
                    }
                  }))
          .setRowSchema(schema)
          .apply(Convert.fromRows(PageViewAggregator.class));
    }
  }
}
