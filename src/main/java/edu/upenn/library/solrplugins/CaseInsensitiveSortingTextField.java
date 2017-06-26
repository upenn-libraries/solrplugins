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
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.TypeAttribute;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.util.Bits;
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
public class CaseInsensitiveSortingTextField extends TextField implements MultiSerializable, FacetPayload<Object> {

  private static final String NORMALIZED_TOKEN_TYPE = "normalized";
  private static final String RAW_TOKEN_TYPE = "filing";
  private static final String PREFIX_TOKEN_TYPE = "prefix";
  private static final String INDEXED_TOKEN_TYPE = "indexed";

  private static final String SERIALIZER_ARGNAME = "serializer";
  private static final String DISPLAYIZER_ARGNAME = "displayizer";
  private static final String PAYLOAD_HANDLER_ARGNAME = "payloadHandler";
  private static final String HIERARCHY_LEVEL_ARGNAME = "hierarchyLevel";
  private static final char DELIM_CHAR = '\u0000';
  private static final int DEFAULT_HIERARCHY_LEVEL = 0;

  private int hierarchyLevel = DEFAULT_HIERARCHY_LEVEL;
  private String delim;
  private byte[] delimBytes;
  private TextTransformer serializer;
  private TextTransformer displayizer;
  private FacetPayload payloadHandler;

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
    if (args.containsKey(PAYLOAD_HANDLER_ARGNAME)) {
      payloadHandler = loader.newInstance(args.remove(PAYLOAD_HANDLER_ARGNAME), FacetPayload.class);
    } else {
      payloadHandler = new DefaultPayloadHandler();
    }
    if (args.containsKey(HIERARCHY_LEVEL_ARGNAME)) {
      hierarchyLevel = loader.newInstance(args.remove(HIERARCHY_LEVEL_ARGNAME), Integer.class);
    }
    delim = initDelim(hierarchyLevel);
    delimBytes = delim.getBytes(StandardCharsets.UTF_8);
    super.init(schema, args);
  }

  @Override
  public String getDelim() {
    return delim;
  }

  @Override
  public BytesRef normalizeQueryTarget(String val, boolean strict, String fieldName) throws IOException {
    return normalizeQueryTarget(val, strict, fieldName, false);
  }

  @Override
  public BytesRef normalizeQueryTarget(String val, boolean strict, String fieldName, boolean appendExtraDelim) throws IOException {
    TokenStream ts = getQueryAnalyzer().tokenStream(fieldName, val);
    try {
      ts.reset();
      CharTermAttribute termAtt = ts.getAttribute(CharTermAttribute.class);
      TypeAttribute typeAtt = ts.getAttribute(TypeAttribute.class);
      String matchType = strict ? INDEXED_TOKEN_TYPE : NORMALIZED_TOKEN_TYPE;
      while (ts.incrementToken()) {
        if (matchType.equals(typeAtt.type())) {
          BytesRefBuilder ret = new BytesRefBuilder();
          ret.copyChars(termAtt.toString());
          if (!strict || appendExtraDelim) {
            ret.append(delimBytes, 0, delimBytes.length);
          }
          return ret.get();
        }
      }
      return new BytesRef(BytesRef.EMPTY_BYTES);
    } finally {
      ts.close();
    }
  }

  @Override
  public void updateExternalRepresentation(NamedList<Object> nl) {
    for (int i = 0; i < nl.size(); i++) {
      String rawName = nl.getName(i);
      String externalName = readableToExternal(rawName);
      nl.setName(i, externalName);
      Object val = nl.getVal(i);
      Object updatedVal;
      if (!(val instanceof Number) && (updatedVal = updateValueExternalRepresentation(val)) != null) {
        nl.setVal(i, updatedVal);
      }
    }
  }

  public String readableToExternal(String indexedForm) {
    int startIndex = indexedForm.indexOf(delim) + hierarchyLevel + 1;
    if (startIndex <= hierarchyLevel) {
      return indexedForm;
    } else {
      int endIndex = indexedForm.indexOf(delim, startIndex);
      if (endIndex < 0) {
        return indexedForm.substring(startIndex);
      } else {
        String displayPart = indexedForm.substring(startIndex, endIndex);
        if ((endIndex += (hierarchyLevel + 1)) >= indexedForm.length()) {
          return displayPart;
        } else {
          return indexedForm.substring(endIndex).concat(displayPart);
        }
      }
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
  public boolean addEntry(String termKey, long count, Term term, List<Entry<LeafReader, Bits>> leaves, NamedList<Object> res) throws IOException {
    return payloadHandler.addEntry(termKey, count, term, leaves, res);
  }

  @Override
  public Entry<String, Object> addEntry(String termKey, long count, Term term, List<Entry<LeafReader, Bits>> leaves) throws IOException {
    return payloadHandler.addEntry(termKey, count, term, leaves);
  }

  @Override
  public Object mergePayload(Object preExisting, Object add, long preExistingCount, long addCount) {
    return payloadHandler.mergePayload(preExisting, add, preExistingCount, addCount);
  }

  @Override
  public long extractCount(Object val) {
    return payloadHandler.extractCount(val);
  }

  @Override
  public Object updateValueExternalRepresentation(Object internal) {
    return payloadHandler.updateValueExternalRepresentation(internal);
  }

  private static class DefaultPayloadHandler implements FacetPayload<Object> {

    @Override
    public boolean addEntry(String termKey, long count, Term term, List<Entry<LeafReader, Bits>> leaves, NamedList<Object> res) throws IOException {
      return false;
    }

    @Override
    public Entry<String, Object> addEntry(String termKey, long count, Term term, List<Entry<LeafReader, Bits>> leaves) throws IOException {
      return null;
    }

    @Override
    public Object mergePayload(Object preExisting, Object add, long preExistingCount, long addCount) {
      return null;
    }

    @Override
    public long extractCount(Object val) {
      return 1L; // for document-centric results only.
    }

    @Override
    public Object updateValueExternalRepresentation(Object internal) {
      return null;
    }

  }
}
