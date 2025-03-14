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

package org.apache.dataimm.infra.solr;

import static java.util.Collections.singletonList;

import java.util.Optional;

import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.impl.Krb5HttpClientBuilder;
import org.apache.solr.common.cloud.SolrZkClient;

public class DataimmSolrCloudClientBuilder {
  private static final String KEYSTORE_LOCATION_ARG = "javax.net.ssl.keyStore";
  private static final String KEYSTORE_PASSWORD_ARG = "javax.net.ssl.keyStorePassword";
  private static final String KEYSTORE_TYPE_ARG = "javax.net.ssl.keyStoreType";
  private static final String TRUSTSTORE_LOCATION_ARG = "javax.net.ssl.trustStore";
  private static final String TRUSTSTORE_PASSWORD_ARG = "javax.net.ssl.trustStorePassword";
  private static final String TRUSTSTORE_TYPE_ARG = "javax.net.ssl.trustStoreType";
  private static final String JAVA_SECURITY_AUTH_LOGIN_CONFIG = "java.security.auth.login.config";
  private static final String SOLR_HTTPCLIENT_BUILDER_FACTORY = "solr.httpclient.builder.factory";

  String zkConnectString;
  String collection;
  String configSet;
  String configDir;
  int shards = 1;
  int replication = 1;
  int retryTimes = 10;
  int interval = 5;
  int maxShardsPerNode = replication * shards;
  String routerName = "implicit";
  String routerField = "_router_field_";
  CloudSolrClient solrCloudClient;
  SolrZkClient solrZkClient;
  boolean implicitRouting;
  String jaasFile;
  String znode;
  String saslUsers;
  String propName;
  String propValue;
  String securityJsonLocation;
  boolean secure;
  String transferMode;
  String copySrc;
  String copyDest;
  String output;
  boolean includeDocNumber;
  String autoScalingJsonLocation;

  public DataimmSolrCloudClient build() {
    return new DataimmSolrCloudClient(this);
  }

  public DataimmSolrCloudClientBuilder withZkConnectString(String zkConnectString) {
    this.zkConnectString = zkConnectString;
    return this;
  }

  public DataimmSolrCloudClientBuilder withCollection(String collection) {
    this.collection = collection;
    return this;
  }

  public DataimmSolrCloudClientBuilder withConfigSet(String configSet) {
    this.configSet = configSet;
    return this;
  }

  public DataimmSolrCloudClientBuilder withConfigDir(String configDir) {
    this.configDir = configDir;
    return this;
  }

  public DataimmSolrCloudClientBuilder withShards(int shards) {
    this.shards = shards;
    return this;
  }

  public DataimmSolrCloudClientBuilder withReplication(int replication) {
    this.replication = replication;
    return this;
  }

  public DataimmSolrCloudClientBuilder withRetry(int retryTimes) {
    this.retryTimes = retryTimes;
    return this;
  }

  public DataimmSolrCloudClientBuilder withInterval(int interval) {
    this.interval = interval;
    return this;
  }

  public DataimmSolrCloudClientBuilder withMaxShardsPerNode(int maxShardsPerNode) {
    this.maxShardsPerNode = maxShardsPerNode;
    return this;
  }

  public DataimmSolrCloudClientBuilder withRouterName(String routerName) {
    this.routerName = routerName;
    return this;
  }

  public DataimmSolrCloudClientBuilder withRouterField(String routerField) {
    this.routerField = routerField;
    return this;
  }

  public DataimmSolrCloudClientBuilder isImplicitRouting(boolean implicitRouting) {
    this.implicitRouting = implicitRouting;
    return this;
  }

  public DataimmSolrCloudClientBuilder withJaasFile(String jaasFile) {
    this.jaasFile = jaasFile;
    setupSecurity(jaasFile);
    return this;
  }

  public DataimmSolrCloudClientBuilder withSolrCloudClient() {
    this.solrCloudClient = new CloudSolrClient.Builder(singletonList(this.zkConnectString), Optional.empty()).build();
    return this;
  }

  public DataimmSolrCloudClientBuilder withSolrZkClient(int zkClientTimeout, int zkClientConnectTimeout) {
    this.solrZkClient = new SolrZkClient(this.zkConnectString, zkClientTimeout, zkClientConnectTimeout);
    return this;
  }

  public DataimmSolrCloudClientBuilder withKeyStoreLocation(String keyStoreLocation) {
    if (keyStoreLocation != null) {
      System.setProperty(KEYSTORE_LOCATION_ARG, keyStoreLocation);
    }
    return this;
  }

  public DataimmSolrCloudClientBuilder withKeyStorePassword(String keyStorePassword) {
    if (keyStorePassword != null) {
      System.setProperty(KEYSTORE_PASSWORD_ARG, keyStorePassword);
    }
    return this;
  }

  public DataimmSolrCloudClientBuilder withKeyStoreType(String keyStoreType) {
    if (keyStoreType != null) {
      System.setProperty(KEYSTORE_TYPE_ARG, keyStoreType);
    }
    return this;
  }

  public DataimmSolrCloudClientBuilder withTrustStoreLocation(String trustStoreLocation) {
    if (trustStoreLocation != null) {
      System.setProperty(TRUSTSTORE_LOCATION_ARG, trustStoreLocation);
    }
    return this;
  }

  public DataimmSolrCloudClientBuilder withTrustStorePassword(String trustStorePassword) {
    if (trustStorePassword != null) {
      System.setProperty(TRUSTSTORE_PASSWORD_ARG, trustStorePassword);
    }
    return this;
  }

  public DataimmSolrCloudClientBuilder withTrustStoreType(String trustStoreType) {
    if (trustStoreType != null) {
      System.setProperty(TRUSTSTORE_TYPE_ARG, trustStoreType);
    }
    return this;
  }

  public DataimmSolrCloudClientBuilder withSaslUsers(String saslUsers) {
    this.saslUsers = saslUsers;
    return this;
  }

  public DataimmSolrCloudClientBuilder withZnode(String znode) {
    this.znode = znode;
    return this;
  }

  public DataimmSolrCloudClientBuilder withClusterPropName(String clusterPropName) {
    this.propName = clusterPropName;
    return this;
  }

  public DataimmSolrCloudClientBuilder withClusterPropValue(String clusterPropValue) {
    this.propValue = clusterPropValue;
    return this;
  }

  public DataimmSolrCloudClientBuilder withTransferMode(String transferMode) {
    this.transferMode = transferMode;
    return this;
  }

  public DataimmSolrCloudClientBuilder withCopySrc(String copySrc) {
    this.copySrc = copySrc;
    return this;
  }

  public DataimmSolrCloudClientBuilder withCopyDest(String copyDest) {
    this.copyDest = copyDest;
    return this;
  }

  public DataimmSolrCloudClientBuilder withOutput(String output) {
    this.output = output;
    return this;
  }

  public DataimmSolrCloudClientBuilder withIncludeDocNumber(boolean includeDocNumber) {
    this.includeDocNumber = includeDocNumber;
    return this;
  }

  public DataimmSolrCloudClientBuilder withSecurityJsonLocation(String securityJson) {
    this.securityJsonLocation = securityJson;
    return this;
  }

  public DataimmSolrCloudClientBuilder withSecure(boolean isSecure) {
    this.secure = isSecure;
    return this;
  }

  private void setupSecurity(String jaasFile) {
    if (jaasFile != null) {
      System.setProperty(JAVA_SECURITY_AUTH_LOGIN_CONFIG, jaasFile);
      System.setProperty(SOLR_HTTPCLIENT_BUILDER_FACTORY, Krb5HttpClientBuilder.class.getCanonicalName());
    }
  }

  public DataimmSolrCloudClientBuilder withAutoScalingJsonLocation(String autoScalingJsonLocation) {
    this.autoScalingJsonLocation = autoScalingJsonLocation;
    return this;
  }
}
