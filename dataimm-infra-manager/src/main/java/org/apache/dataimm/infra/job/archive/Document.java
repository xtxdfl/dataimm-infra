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

import static java.util.Collections.unmodifiableMap;

import java.util.HashMap;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonAnyGetter;
import com.fasterxml.jackson.annotation.JsonAnySetter;

public class Document {
  private final Map<String, Object> fieldMap;

  private Document() {
    fieldMap = new HashMap<>();
  }

  public Document(Map<String, Object> fieldMap) {
    this.fieldMap = unmodifiableMap(fieldMap);
  }

  public String getString(String key) {
    Object value = fieldMap.get(key);
    return value == null ? null : value.toString();
  }

  @JsonAnyGetter
  public Map<String, Object> getFieldMap() {
    return fieldMap;
  }

  @JsonAnySetter
  private void put(String key, Object value) {
    fieldMap.put(key, value);
  }
}
