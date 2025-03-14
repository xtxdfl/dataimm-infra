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

import org.apache.dataimm.infra.job.CloseableIterator;
import org.apache.dataimm.infra.job.ObjectSource;

public class SolrDocumentSource implements ObjectSource<Document> {
  private final SolrDAO solrDAO;
  private final String start;
  private final String end;

  public SolrDocumentSource(SolrDAO solrDAO, String start, String end) {
    this.solrDAO = solrDAO;
    this.start = start;
    this.end = end;
  }

  @Override
  public CloseableIterator<Document> open(Document current, int rows) {
    return solrDAO.query(start, end, current, rows);
  }
}
