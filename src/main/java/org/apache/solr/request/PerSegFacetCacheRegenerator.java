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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.WeakHashMap;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.solr.request.DocValuesFacets.SegmentCacheEntry;
import org.apache.solr.search.CacheRegenerator;
import org.apache.solr.search.SolrCache;
import org.apache.solr.search.SolrIndexSearcher;

/**
 *
 * @author magibney
 */
public class PerSegFacetCacheRegenerator implements CacheRegenerator {
  private final Map<SolrIndexSearcher, Set<Object>> activeSegments;

  public PerSegFacetCacheRegenerator() {
    activeSegments = new WeakHashMap<>();
  }

  @Override
  public boolean regenerateItem(SolrIndexSearcher newSearcher, SolrCache nc, SolrCache oc, Object oldKey, Object oldVal) throws IOException {
    Set<Object> segmentKeys = activeSegments.get(newSearcher);
    if (segmentKeys == null) {
      synchronized (activeSegments) {
        segmentKeys = activeSegments.get(newSearcher);
        if (segmentKeys == null) {
          List<LeafReaderContext> leaves = newSearcher.getTopReaderContext().leaves();
          segmentKeys = new HashSet<>(leaves.size());
          for (LeafReaderContext leaf : leaves) {
            segmentKeys.add(leaf.reader().getCombinedCoreAndDeletesKey());
          }
          activeSegments.clear();
          activeSegments.put(newSearcher, segmentKeys);
        }
      }
    }
    Map<Object, SegmentCacheEntry> oldSegmentCache = (Map<Object, SegmentCacheEntry>)oldVal;
    Map<Object, SegmentCacheEntry> newSegmentCache = new HashMap<>(segmentKeys.size());
    for (Map.Entry<Object, SegmentCacheEntry> e : oldSegmentCache.entrySet()) {
      Object segmentKey = e.getKey();
      if (segmentKeys.contains(segmentKey)) {
        newSegmentCache.put(segmentKey, e.getValue());
      }
    }
    if (!newSegmentCache.isEmpty()) {
      nc.put(oldKey, newSegmentCache);
    }
    return true;
  }
}
