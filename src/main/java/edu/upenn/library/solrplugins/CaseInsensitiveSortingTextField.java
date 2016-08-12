/*
 * Copyright 2016 The Trustees of the University of Pennsylvania
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0

 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.upenn.library.solrplugins;

import java.io.IOException;
import java.io.StringReader;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Map;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.TypeAttribute;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.CharsRef;
import org.apache.lucene.util.CharsRefBuilder;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.SolrResourceLoader;
import org.apache.solr.request.FacetPayload;
import org.apache.solr.request.MultiSerializable;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.schema.TextField;

/**
 *
 * @author michael
 */
public class CaseInsensitiveSortingTextField extends TextField implements MultiSerializable, FacetPayload {

  private static final String NORMALIZED_TOKEN_TYPE = "normalized";
  private static final String RAW_TOKEN_TYPE = "raw";
  private static final String INDEXED_TOKEN_TYPE = "indexed";

  private static final String SERIALIZER_ARGNAME = "serializer";
  private static final String DISPLAYIZER_ARGNAME = "displayizer";
  private static final String HIERARCHY_LEVEL_ARGNAME = "hierarchyLevel";
  private static final char DELIM_CHAR = '\u0000';
  private static final int DEFAULT_HIERARCHY_LEVEL = 0;

  private int hierarchyLevel = DEFAULT_HIERARCHY_LEVEL;
  private String delim;
  private byte[] delimBytes;
  private TextTransformer serializer;
  private TextTransformer displayizer;

  private String initDelim(int hierarchyLevel) {
    char[] tmp = new char[hierarchyLevel + 1];
    Arrays.fill(tmp, DELIM_CHAR);
    return new String(tmp);
  }

  @Override
  protected void init(IndexSchema schema, Map<String, String> args) {
    SolrResourceLoader loader = schema.getResourceLoader();
    if (args.containsKey(SERIALIZER_ARGNAME)) {
      serializer = loader.newInstance(args.remove(SERIALIZER_ARGNAME), TextTransformer.class);
    }
    if (args.containsKey(DISPLAYIZER_ARGNAME)) {
      displayizer = loader.newInstance(args.remove(DISPLAYIZER_ARGNAME), TextTransformer.class);
    }
    if (args.containsKey(HIERARCHY_LEVEL_ARGNAME)) {
      hierarchyLevel = loader.newInstance(args.remove(HIERARCHY_LEVEL_ARGNAME), Integer.class);
    }
    delim = initDelim(hierarchyLevel);
    delimBytes = delim.getBytes(StandardCharsets.UTF_8);
    super.init(schema, args);
  }

  @Override
  public BytesRef normalizeQueryTarget(String val, boolean strict, String fieldName) throws IOException {
    TokenStream ts = getQueryAnalyzer().tokenStream(fieldName, new StringReader(val));
    ts.reset();
    CharTermAttribute termAtt = ts.getAttribute(CharTermAttribute.class);
    TypeAttribute typeAtt = ts.getAttribute(TypeAttribute.class);
    String matchType = strict ? INDEXED_TOKEN_TYPE : NORMALIZED_TOKEN_TYPE;
    while (ts.incrementToken()) {
      if (matchType.equals(typeAtt.type())) {
        BytesRefBuilder ret = new BytesRefBuilder();
        ret.copyChars(termAtt.toString());
        ret.append(delimBytes, 0, delimBytes.length);
        return ret.get();
      }
    }
    throw new IllegalStateException("no token of type " + matchType + " found for field " + fieldName + ", val=" + val);
  }

  @Override
  public void updateExternalRepresentation(NamedList<Object> nl) {
    for (int i = 0; i < nl.size(); i++) {
      String rawName = nl.getName(i);
      String externalName = readableToExternal(rawName);
      nl.setName(i, externalName);
    }
  }

  public String readableToExternal(String indexedForm) {
    int startIndex = indexedForm.indexOf(delim) + hierarchyLevel + 1;
    if (startIndex <= hierarchyLevel) {
      return indexedForm;
    } else {
      int endIndex = indexedForm.indexOf(delim, startIndex);
      return endIndex < 0 ? indexedForm.substring(startIndex) : indexedForm.substring(startIndex, endIndex);
    }
  }

  private int delimOffset(BytesRef br) {
    return delimOffset(br, br.offset);
  }

  private int delimOffset(BytesRef br, int startOffset) {
    byte[] bytes = br.bytes;
    int limit = br.offset + br.length;
    int match = 0;
    for (int i = startOffset; i < limit; i++) {
      if (bytes[i] == DELIM_CHAR) {
        if (++match > hierarchyLevel) {
          return i - hierarchyLevel;
        }
      } else if (match > 0) {
        match = 0;
      }
    }
    return -1;
  }

  private int delimOffset(CharsRef cr) {
    return delimOffset(cr, cr.offset);
  }

  private int delimOffset(CharsRef cr, int startOffset) {
    char[] chars = cr.chars;
    int limit = cr.offset + cr.length;
    int match = 0;
    for (int i = startOffset; i < limit; i++) {
      if (chars[i] == DELIM_CHAR) {
        if (++match > hierarchyLevel) {
          return i - hierarchyLevel;
        }
      } else if (match > 0) {
        match = 0;
      }
    }
    return -1;
  }

  @Override
  public CharsRef indexedToNormalized(BytesRef input, CharsRefBuilder output) {
    int endIndex = delimOffset(input);
    if (endIndex < 0) {
      return super.indexedToReadable(input, output);
    } else {
      return super.indexedToReadable(new BytesRef(input.bytes, input.offset, endIndex - input.offset), output);
    }
  }

  @Override
  public String indexedToNormalized(String indexedForm) {
    int endIndex = indexedForm.indexOf(delim);
    if (endIndex < 0) {
      return indexedForm;
    } else {
      return indexedForm.substring(0, endIndex);
    }
  }

  @Override
  public CharsRef readableToDisplay(CharsRef input) {
    return displayizer == null ? input : displayizer.transform(input);
  }

  @Override
  public String readableToDisplay(String input) {
    return displayizer == null ? input : displayizer.transform(input);
  }

  @Override
  public CharsRef readableToSerialized(CharsRef input) {
    return serializer == null ? input : serializer.transform(input);
  }

  @Override
  public String readableToSerialized(String input) {
    return serializer == null ? input : serializer.transform(input);
  }

  @Override
  public boolean addEntry(String value, int count, PostingsEnum postings, NamedList res) throws IOException {
    // proof-of-concept implementation
    NamedList<Object> entry = new NamedList<>();
    entry.add("count", count);
    res.add(value, entry);
    int i = -1;
    while (postings.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
      i++;
      NamedList<Object> documentEntry = new NamedList<>();
      entry.add("doc"+i, documentEntry);
      for (int j = 0; j < postings.freq(); j++) {
        postings.nextPosition();
        String extra = postings.getPayload().utf8ToString();
        documentEntry.add("position"+j, extra);
      }
    }
    return true;
  }

  @Override
  public NamedList<Object> mergePayload(NamedList<Object> preExisting, NamedList<Object> add) {
    long addCount = ((Number) add.remove("count")).longValue();
    int countIndex = preExisting.indexOf("count", 0);
    long preCount = ((Number) preExisting.getVal(countIndex)).longValue();
    preExisting.setVal(countIndex, preCount + addCount);
    preExisting.addAll(add);
    return preExisting;
  }

}
