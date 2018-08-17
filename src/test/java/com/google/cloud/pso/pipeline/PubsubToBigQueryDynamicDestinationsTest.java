/*
 * Copyright (C) 2018 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.google.cloud.pso.pipeline;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;

import com.google.api.services.bigquery.model.TableRow;
import com.google.common.collect.ImmutableMap;
import java.nio.charset.Charset;
import org.apache.beam.sdk.io.gcp.bigquery.TableDestination;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.values.ValueInSingleWindow;
import org.joda.time.Instant;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Test cases for the {@link com.google.cloud.pso.pipeline.PubsubToBigQueryDynamicDestinations}
 * class.
 */
@RunWith(JUnit4.class)
public class PubsubToBigQueryDynamicDestinationsTest {

  @Rule public ExpectedException expectedException = ExpectedException.none();

  /**
   * Tests that {@link PubsubToBigQueryDynamicDestinations#convertJsonToTableRow} successfully
   * converts valid JSON to {@link TableRow} objects.
   */
  @Test
  public void testConvertJsonToTableRow() {
    // Arrange
    //
    String json = "{\"id\": 123, \"name\": \"Ryan\"}";

    // Act
    //
    TableRow row = PubsubToBigQueryDynamicDestinations.convertJsonToTableRow(json);

    // Assert
    //
    assertThat(row, is(notNullValue()));
    assertThat(row.get("id"), is(equalTo(123)));
    assertThat(row.get("name"), is(equalTo("Ryan")));
  }

  /**
   * Tests that {@link PubsubToBigQueryDynamicDestinations#convertJsonToTableRow} throws a {@link
   * RuntimeException} when the JSON to process is invalid.
   */
  @Test
  public void testConvertInvalidJsonToTableRow() {
    // Arrange
    //
    String json = "{\"id\": 123, \"name\": \"Ry";

    // Act
    //
    expectedException.expect(RuntimeException.class);
    PubsubToBigQueryDynamicDestinations.convertJsonToTableRow(json);
  }

  /**
   * Tests that {@link PubsubToBigQueryDynamicDestinations#getTableDestination} successfully
   * converts a message's table attribute into a valid {@link
   * org.apache.beam.sdk.io.gcp.bigquery.TableDestination}.
   */
  @Test
  public void testGetTableDestination() {
    // Arrange
    //
    String tableAttr = "table";
    String project = "project-id";
    String dataset = "demo";
    String table = "destination_1";

    byte[] payload = "{\"id\": 123, \"name\": \"Ryan\"}".getBytes(Charset.defaultCharset());
    ImmutableMap<String, String> attributes = ImmutableMap.of(tableAttr, table);

    PubsubMessage message = new PubsubMessage(payload, attributes);

    ValueInSingleWindow value =
        ValueInSingleWindow.of(message, Instant.now(), GlobalWindow.INSTANCE, PaneInfo.NO_FIRING);

    // Act
    //
    TableDestination tableDestination =
        PubsubToBigQueryDynamicDestinations.getTableDestination(value, tableAttr, project, dataset);

    // Assert
    //
    assertThat(tableDestination, is(notNullValue()));
    assertThat(
        tableDestination.getTableSpec(),
        is(equalTo(String.format("%s:%s.%s", project, dataset, table))));
  }

  /**
   * Tests that {@link PubsubToBigQueryDynamicDestinations#getTableDestination} throws a {@link
   * RuntimeException} when passed a null message.
   */
  @Test
  public void testGetTableDestinationNullMessage() {
    // Arrange
    //
    String tableAttr = "table";
    String project = "project-id";
    String dataset = "demo";
    String table = "destination_1";

    ValueInSingleWindow value =
        ValueInSingleWindow.of(
            (PubsubMessage) null, Instant.now(), GlobalWindow.INSTANCE, PaneInfo.NO_FIRING);

    // Act
    //
    expectedException.expect(RuntimeException.class);
    PubsubToBigQueryDynamicDestinations.getTableDestination(value, tableAttr, project, dataset);
  }
}
