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
import org.apache.dataimm.infra.solr.DataimmSolrCloudClientException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractRetryCommand<RESPONSE> {
  private static final Logger logger = LoggerFactory.getLogger(AbstractRetryCommand.class);

  private final int interval;
  private final int maxRetries;

  public AbstractRetryCommand(int maxRetries, int interval) {
    this.maxRetries = maxRetries;
    this.interval = interval;
  }

  public abstract RESPONSE createAndProcessRequest(DataimmSolrCloudClient solrCloudClient) throws Exception;

  public RESPONSE run(DataimmSolrCloudClient solrCloudClient) throws Exception {
    return retry(0, solrCloudClient);
  }

  private RESPONSE retry(int tries, DataimmSolrCloudClient solrCloudClient) throws Exception {
    try {
      return createAndProcessRequest(solrCloudClient);
    } catch (Exception ex) {
      logger.error(ex.getMessage(), ex);
      tries++;
      logger.info("Command failed, tries again (tries: {})", tries);
      if (maxRetries == tries) {
        throw new DataimmSolrCloudClientException(String.format("Maximum retries exceeded: %d", tries), ex);
      } else {
        Thread.sleep(interval * 1000);
        return retry(tries, solrCloudClient);
      }
    }
  }
}
