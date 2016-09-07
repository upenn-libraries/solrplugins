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
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Deque;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.CharsRefBuilder;
import org.apache.lucene.util.UnicodeUtil;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.request.BidirectionalFacetResponseBuilder.BaseLocalTermEnv;
import org.apache.solr.request.BidirectionalFacetResponseBuilder.BaseTermIndexKey;
import org.apache.solr.request.BidirectionalFacetResponseBuilder.LimitMinder;
import org.apache.solr.schema.FieldType;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.search.DocList;
import org.apache.solr.search.DocSet;
import org.apache.solr.search.SolrIndexSearcher;

/**
 *
 * @author magibney
 */
public class DocBasedFacetResponseBuilder {
  
  public static class TermDocIndexKey extends BaseTermIndexKey<TermDocIndexKey> {

    public final BytesRef docId;
    
    public TermDocIndexKey(int index, BytesRef docId) {
      super(index);
      this.docId = docId;
    }

    @Override
    public String toString() {
      return TermDocIndexKey.class.getSimpleName() + "(index=" + index + ", docId=" + docId + ')';
    }
    
  }
  
  public static class LocalDocEnv<T extends FieldType & FacetPayload> extends BaseLocalTermEnv<T, TermDocIndexKey> {

    private final BytesRef targetDoc;
    private final DocSet docs;
    private final Sort sort;
    private final SortField sortField;
    private final Comparator<BytesRef> idFieldComparator;
    private final String idField;
    
    private TermDocIndexKey termDocIndexKey;
    
    private int activeTermIndex = -1;
    private Document[] documents = null;
    private BytesRef[] docIds = null;
    
    private int localDocIndex = -1;

    public LocalDocEnv(int offset, int limit, int startTermIndex, int adjust, int targetIdx, String targetDoc, int nTerms,
        String contains, boolean ignoreCase, int mincount, int[] counts, CharsRefBuilder charsRef, boolean extend,
        SortedSetDocValues si, SolrIndexSearcher searcher, DocSet docs, String fieldName, T ft, NamedList res) {
      super(offset, limit, startTermIndex, adjust, targetIdx, nTerms, contains, ignoreCase, mincount, counts,
          charsRef, extend, si, searcher, fieldName, ft, res);
      SchemaField uniqueKeyField = searcher.getSchema().getUniqueKeyField();
      this.targetDoc = new BytesRef(targetDoc);
      this.idField = uniqueKeyField.getName();
      this.sortField = uniqueKeyField.getSortField(true);
      this.idFieldComparator = this.sortField.getBytesComparator();
      this.sort = new Sort(sortField);
      this.docs = docs;
    }
    
    private boolean initTermIndex(int termIndex) {
      if (currentTermBytes == null) {
        if (termIndex < adjust || termIndex >= nTerms || !acceptTerm(termIndex)) {
          return false;
        }
      }
      try {
        //TODO get this some other way.
        Query q = new TermQuery(new Term(fieldName, currentTermBytes));
        DocList docList = searcher.getDocList(q, docs, sort, 0, Integer.MAX_VALUE); //TODO page through? avoid OOM for large doc counts?
        int size = docList.size();
        if (size <= 0) {
          return false;
        }
        activeTermIndex = termIndex;
        documents = new Document[size];
        docIds = new BytesRef[size];
        searcher.readDocs(documents, docList, Collections.singleton(idField));
        localDocIndex = -1;
        for (int i = 0; i < size; i++) {
          docIds[i] = new BytesRef(documents[i].get(idField));
        }
        return true;
      } catch (IOException ex) {
        throw new RuntimeException(ex);
      }
    }
        
    private int acceptDoc(int termIndex, BytesRef docId) {
      if (activeTermIndex != termIndex) {
        if (!initTermIndex(termIndex)) {
          return -1;
        }
      }
      return docIndex(docId);
    }
    
    private int docIndex(BytesRef docId) {
      if (localDocIndex >= 0 && localDocIndex < docIds.length && docId.bytesEquals(docIds[localDocIndex])) {
        return localDocIndex;
      }
      return Arrays.binarySearch(docIds, docId, idFieldComparator);
    }
    
    private BytesRef incrementDocId(int termIndex, BytesRef lastDocId) {
      if (activeTermIndex != termIndex) {
        if (!initTermIndex(termIndex)) {
          return null;
        }
      }
      int lastDocIndex = docIndex(lastDocId);
      int nextDocIndex;
      if (lastDocIndex < 0) {
        nextDocIndex = ~lastDocIndex;
      } else {
        nextDocIndex = lastDocIndex + 1;
      }
      if (nextDocIndex < docIds.length) {
        localDocIndex = nextDocIndex;
        return docIds[nextDocIndex];
      } else {
        localDocIndex = -1;
        return null;
      }
    }

    @Override
    public TermDocIndexKey incrementKey(TermDocIndexKey previousKey) {
      int termIndex = previousKey.index;
      BytesRef docId = previousKey.docId;
      do {
        while ((docId = incrementDocId(termIndex, docId)) != null) {
          int docIndex = acceptDoc(termIndex, docId);
          if (docIndex >= 0) {
            localDocIndex = docIndex;
            return termDocIndexKey = new TermDocIndexKey(termIndex, docId);
          }
        }
        docId = new BytesRef(BytesRef.EMPTY_BYTES);
      } while ((termIndex = incrementTermIndex(termIndex)) >= 0);
      return null;
    }
    
    private BytesRef decrementDocId(int termIndex, BytesRef lastDocId) {
      if (activeTermIndex != termIndex) {
        if (!initTermIndex(termIndex)) {
          return null;
        }
      }
      int nextDocIndex = docIndex(lastDocId) - 1;
      if (nextDocIndex >= 0) {
        localDocIndex = nextDocIndex;
        return docIds[nextDocIndex];
      } else {
        localDocIndex = -1;
        return null;
      }
    }

    @Override
    public TermDocIndexKey decrementKey(TermDocIndexKey previousKey) {
      int termIndex = previousKey.index;
      BytesRef docId = previousKey.docId;
      do {
        while ((docId = decrementDocId(termIndex, docId)) != null) {
          int docIndex = acceptDoc(termIndex, docId);
          if (docIndex >= 0) {
            localDocIndex = docIndex;
            return termDocIndexKey = new TermDocIndexKey(termIndex, docId);
          }
        }
        docId = UnicodeUtil.BIG_TERM;
      } while ((termIndex = decrementTermIndex(termIndex)) >= 0);
      return null;
    }

    @Override
    public void addEntry(LimitMinder<T, TermDocIndexKey> limitMinder, TermDocIndexKey facetKey, Deque<Map.Entry<String, Object>> entryBuilder) throws IOException {
      if (termDocIndexKey != facetKey) {
        throw new IllegalStateException();
      }
      String termDocTerm = currentTerm + '\u0000' + facetKey.docId.utf8ToString();
      Entry<String, Object> entry = new SimpleImmutableEntry<>(termDocTerm, documents[localDocIndex]);
      limitMinder.addEntry(entry, entryBuilder);
    }

    @Override
    public TermDocIndexKey targetKey() throws IOException {
      return new TermDocIndexKey(getTargetKeyIndex(), targetDoc);
    }

    @Override
    public TermDocIndexKey targetKeyInit(boolean ascending) throws IOException {
      int termIndex = getTargetKeyIndexInit(ascending);
      if (termIndex < 0) {
        return null;
      }
      int rawTargetIdx = getTargetKeyIndex();
      BytesRef initTargetDoc = targetDoc;
      if (rawTargetIdx < termIndex) {
        initTargetDoc = UnicodeUtil.BIG_TERM;
      } else if (rawTargetIdx > termIndex) {
        initTargetDoc = new BytesRef(BytesRef.EMPTY_BYTES);
      }
      TermDocIndexKey ret = new TermDocIndexKey(termIndex, initTargetDoc);
      int docIndex = acceptDoc(termIndex, initTargetDoc);
      if (docIndex >= 0) {
        localDocIndex = docIndex;
        return termDocIndexKey = ret;
      } else if (ascending) {
        return incrementKey(ret);
      } else {
        return decrementKey(ret);
      }
    }

  }
}
