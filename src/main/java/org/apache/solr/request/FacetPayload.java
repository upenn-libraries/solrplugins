/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.solr.request;

import java.io.IOException;
import java.util.Map.Entry;
import org.apache.lucene.index.PostingsEnum;
import org.apache.solr.common.util.NamedList;

/**
 *
 * @author magibney
 */
public interface FacetPayload<T> {
  long extractCount(T val);
  boolean addEntry(String termKey, long count, PostingsEnum postings, NamedList<T> res) throws IOException;
  Entry<String, T> addEntry(String termKey, long count, PostingsEnum postings) throws IOException;
  T mergePayload(T preExisting, T add, long preExistingCount, long addCount);
  Object updateValueExternalRepresentation(T internal);
}
