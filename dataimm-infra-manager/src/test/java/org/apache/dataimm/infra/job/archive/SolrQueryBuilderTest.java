/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.dataimm.infra.job.archive;

import static org.apache.dataimm.infra.job.archive.SolrQueryBuilder.computeEnd;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.Assert.assertThat;

import java.time.Duration;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.HashMap;

import org.apache.solr.client.solrj.SolrQuery;
import org.hamcrest.core.Is;
import org.junit.Test;

public class SolrQueryBuilderTest {
  private static final Document DOCUMENT = new Document(new HashMap<String, Object>() {{
    put("logtime", "2017-10-02'T'10:00:11.634Z");
    put("id", "1");
  }});

  @Test
  public void testDefaultQuery() throws Exception {
    SolrQuery solrQuery = new SolrQueryBuilder()
            .build();
    assertThat(solrQuery.getQuery(), is("*:*"));
  }

  @Test
  public void testSetQueryReplacesTheDefaultQueryTextAndParameterPlaceholdersAreReplacedToValues() throws Exception {
    SolrQuery solrQuery = new SolrQueryBuilder()
            .setQueryText("logtime:[* TO ${end}]")
            .setInterval(null, "2017-11-27'T'10:12:11.372Z")
            .build();
    assertThat(solrQuery.getQuery(), is("logtime:[* TO 2017\\-11\\-27'T'10\\:12\\:11.372Z]"));
  }

  @Test
  public void testSetFilterQueryAddsAFilterQueryAndParameterPlaceholdersAreReplacedToValues() throws Exception {
    SolrQuery solrQuery = new SolrQueryBuilder()
            .setFilterQueryText("(logtime:${logtime} AND id:{${id} TO *]) OR logtime:{${logtime} TO ${end}]")
            .setDocument(DOCUMENT)
            .setInterval(null, "2017-11-27'T'10:12:11.372Z")
            .build();
    assertThat(solrQuery.getFilterQueries()[0], is( "(logtime:2017\\-10\\-02'T'10\\:00\\:11.634Z AND id:{1 TO *]) OR logtime:{2017\\-10\\-02'T'10\\:00\\:11.634Z TO 2017\\-11\\-27'T'10\\:12\\:11.372Z]"));
  }

  @Test
  public void testSetFilterQueryWhenDocumentIsNull() throws Exception {
    SolrQuery solrQuery = new SolrQueryBuilder()
            .setFilterQueryText("(logtime:\"${logtime}\" AND id:{\"${id}\" TO *]) OR logtime:{\"${logtime}\" TO \"${end}\"]")
            .setInterval(null, "2017-11-27'T'10:12:11.372Z")
            .build();
    assertThat(solrQuery.getFilterQueries(), is(nullValue()));
  }

  @Test
  public void testNullEndValueDoesNotAffectFilterQuery() throws Exception {
    SolrQuery solrQuery = new SolrQueryBuilder()
            .setFilterQueryText("logtime:${logtime} AND id:{${id} TO *]")
            .setDocument(DOCUMENT)
            .build();
    assertThat(solrQuery.getFilterQueries()[0], is("logtime:2017\\-10\\-02'T'10\\:00\\:11.634Z AND id:{1 TO *]"));
  }

  @Test
  public void testSetFilterQueryWhenQueryFilterIsNullButDocumentIsNot() throws Exception {
    SolrQuery solrQuery = new SolrQueryBuilder()
            .setDocument(DOCUMENT)
            .build();
    assertThat(solrQuery.getFilterQueries(), is(nullValue()));
  }

  @Test
  public void testSort() throws Exception {
    SolrQuery solrQuery = new SolrQueryBuilder().addSort("logtime", "id").build();
    assertThat(solrQuery.getSorts().get(0).getItem(), is("logtime"));
    assertThat(solrQuery.getSorts().get(1).getItem(), is("id"));
  }

  @Test
  public void test_start_and_end_values_are_given() throws Exception {
    SolrQuery solrQuery = new SolrQueryBuilder().setQueryText("id:[\"${start}\" TO \"${end}\"]").setInterval("10", "13").build();
    assertThat(solrQuery.getQuery(), is("id:[\"10\" TO \"13\"]"));
  }

  @Test
  public void test_start_and_end_values_are_null() throws Exception {
    SolrQuery solrQuery = new SolrQueryBuilder().setQueryText("id:[${start} TO ${end}]").build();
    assertThat(solrQuery.getQuery(), is("id:[* TO *]"));
  }

  @Test
  public void testComputeEndReturnsNullIsNoEndAndNoTTLWasGiven() {
    assertThat(computeEnd(null, OffsetDateTime.now(), null), Is.is(nullValue()));
  }

  @Test
  public void testComputeEndReturnsEndIfOnlyEndWasGiven() {
    String end = "2018-10-09T10:11:12.000Z";
    assertThat(computeEnd(end, OffsetDateTime.now(), null), Is.is(end));
  }

  @Test
  public void testComputeEndReturnsNowMinusTtlIfOnlyTtlWasGiven() {
    OffsetDateTime now = OffsetDateTime.of(2018, 10, 9, 10, 11, 12, 0, ZoneOffset.UTC);
    assertThat(computeEnd(null, now, Duration.ofDays(5)), Is.is("2018-10-04T10:11:12.000Z"));
  }

  @Test
  public void testComputeEndReturnsEndIfBothWasGiven() {
    String end = "2018-10-09T10:11:12.000Z";
    assertThat(computeEnd(end, OffsetDateTime.now(), Duration.ofDays(5)), Is.is(end));
  }
}
