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
package com.beamlytics.inventory.businesslogic.core.transforms.clickstream.validation;

import com.beamlytics.inventory.businesslogic.core.transforms.clickstream.ValidateAndCorrectCSEvt;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.Row;

import java.util.Collection;

public class ValidationUtils {
  public static Schema getValidationWrapper(Schema rowSchema) {

    return Schema.builder()
        .addRowField("data", rowSchema)
        .addField("errors", FieldType.array(FieldType.STRING).withNullable(true))
        .build();
  }

  public static class ValidationRouter extends DoFn<Row, Row> {

    @ProcessElement
    public void process(@Element Row input, MultiOutputReceiver o) {

      Collection<Object> errors = input.getArray("errors");

      if (errors == null || errors.size() < 1) {
        o.get(ValidateAndCorrectCSEvt.MAIN).output(input);
        return;
      }
      o.get(ValidateAndCorrectCSEvt.NEEDS_CORRECTIONS).output(input);
    }
  }
}
