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
package org.apache.dataimm.infra.solr.commands;

import org.apache.dataimm.infra.solr.DataimmSolrCloudClient;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.ZkStateReader;

import java.util.Collection;

public class GetShardsCommand extends AbstractRetryCommand<Collection<Slice>> {

  public GetShardsCommand(int maxRetries, int interval) {
    super(maxRetries, interval);
  }

  @Override
  public Collection<Slice> createAndProcessRequest(DataimmSolrCloudClient solrCloudClient) throws Exception {
    ZkStateReader zkReader = new ZkStateReader(solrCloudClient.getSolrZkClient());
    zkReader.createClusterStateWatchersAndUpdate();
    return zkReader.getClusterState().getCollection(solrCloudClient.getCollection()).getSlices();
  }
}
