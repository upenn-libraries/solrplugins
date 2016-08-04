package edu.upenn.library.solrplugins;

import java.util.Arrays;
import java.util.Map;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.CharsRef;
import org.apache.lucene.util.CharsRefBuilder;
import org.apache.solr.core.SolrResourceLoader;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.schema.TextField;

/**
 *
 * @author michael
 */
public class CaseInsensitiveSortingTextField extends TextField implements MultiSerializable {

  private static final String SERIALIZER_ARGNAME = "serializer";
  private static final String DISPLAYIZER_ARGNAME = "displayizer";
  private static final String HIERARCHY_LEVEL_ARGNAME = "hierarchyLevel";
  private static final char DELIM_CHAR = '\u0000';
  private static final int DEFAULT_HIERARCHY_LEVEL = 0;

  private int hierarchyLevel = DEFAULT_HIERARCHY_LEVEL;
  private String delim;
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
    super.init(schema, args);
  }

  @Override
  public String indexedToReadable(String indexedForm) {
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
  public CharsRef indexedToReadable(BytesRef input, CharsRefBuilder output) {
    int startOffset = delimOffset(input) + hierarchyLevel + 1;
    if (startOffset <= hierarchyLevel) {
      return super.indexedToReadable(input, output);
    } else {
      int endOffset = delimOffset(input, startOffset);
      if (endOffset < 0) {
        return super.indexedToReadable(new BytesRef(input.bytes, startOffset, input.offset + input.length - startOffset), output);
      } else {
        return super.indexedToReadable(new BytesRef(input.bytes, startOffset, endOffset - startOffset), output);
      }
    }
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

}
