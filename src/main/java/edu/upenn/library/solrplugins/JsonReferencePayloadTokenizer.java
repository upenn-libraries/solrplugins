package edu.upenn.library.solrplugins;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.PayloadAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.analysis.tokenattributes.TypeAttribute;
import org.apache.lucene.util.AttributeFactory;
import org.apache.lucene.util.BytesRef;

/**
 * Tokenizer that deserializes a JSON object from a string,
 * and streams both its "raw" object field and the target values
 * in its "refs" nested object. The JSON object should have
 * the following structure:
 * {
 *   "raw": "Georg Wilhelm Friedrich Hegel",
 *   "refs": {
 *     "use_for": ["G. W. F. Hegel", {
 *       "prefix": "Mister",
 *       "filing": "Hegel"
 *     }],
 *     "see_also": ["Hegelianism"]
 *   }
 * }
 *
 * Note that name strings can be either a regular string or a
 * "multi-part string", which is a JSON object that has
 * "prefix" and "filing" keys.
 *
 * The stream from this tokenizer consists of all these terms
 * typed as "normalized", "prefix," and "filing". Use a
 * TokenTypeJoinFilter to join these into a single term
 * which should be suitable for normalized sorting and which
 * can be parsed for facet payloads.
 *
 * @author jeffchiu
 */
public final class JsonReferencePayloadTokenizer extends Tokenizer {

  private static final JsonFactory jsonFactory = new JsonFactory();
  public static final String PAYLOAD_ATTR_SEPARATOR = "\u0000";
  private static final String FIELD_RAW = "raw";
  private static final String FIELD_REFS = "refs";
  private static final String MULTIPART_STRING_PREFIX = "prefix";
  private static final String MULTIPART_STRING_FILING = "filing";
  public static final String TYPE_NORMALIZED = "normalized";
  public static final String TYPE_PREFIX = MULTIPART_STRING_PREFIX;
  public static final String TYPE_FILING = MULTIPART_STRING_FILING;

  private final CharTermAttribute termAtt = addAttribute(CharTermAttribute.class);
  private final TypeAttribute typeAtt = addAttribute(TypeAttribute.class);
  private final PositionIncrementAttribute posIncrAtt = addAttribute(PositionIncrementAttribute.class);
  // I couldn't get this custom ReferenceAttribute to work.
  //private final ReferenceAttribute refAtt = addAttribute(ReferenceAttribute.class);
  private final PayloadAttribute payloadAtt = addAttribute(PayloadAttribute.class);

  private boolean consumed = false;
  private JsonParser parser;
  private List<Token> tokens = new ArrayList<>();
  private Iterator<Token> tokensIter;

  /** Represents a token for this Tokenizer to emit in its stream */
  class Token {
    String term;
    String type;
    String payload; // present if token is a reference target
    int positionIncrement;
  }

  public JsonReferencePayloadTokenizer() {
    super();
  }

  public JsonReferencePayloadTokenizer(AttributeFactory factory) {
    super(factory);
  }

  /**
   * TODO: how to normalize?
   */
  private String normalize(MultiPartString s) {
    return s.getFiling();
  }

  /**
   * Creates tokens for the passed-in MultiPartString
   * and appends them to this object's internal list.
   */
  private void appendTokens(MultiPartString multiPartString, String payload, int positionIncrement) {
    Token normToken = new Token();
    normToken.term = multiPartString.getNormalized();
    normToken.type = TYPE_NORMALIZED;
    normToken.payload = payload;
    normToken.positionIncrement = positionIncrement;
    tokens.add(normToken);

    Token filingToken = new Token();
    filingToken.term = multiPartString.getFiling();
    filingToken.type = TYPE_FILING;
    filingToken.payload = payload;
    filingToken.positionIncrement = 0;
    tokens.add(filingToken);

    if(multiPartString.getPrefix() != null) {
      Token prefixToken = new Token();
      prefixToken.term = multiPartString.getPrefix();
      prefixToken.type = TYPE_PREFIX;
      prefixToken.payload = payload;
      prefixToken.positionIncrement = 0;
      tokens.add(prefixToken);
    }
  }

  private void parse() throws IOException {
    if (parser.nextToken() != JsonToken.START_OBJECT) {
      throw new IOException("Expected data to start with a START_OBJECT token, but found this instead: " + parser.getCurrentToken());
    }

    MultiPartString raw = null;
    int positionIncrement = 1;

    while (parser.nextToken() != JsonToken.END_OBJECT) {
      String topLevelField = parser.getCurrentName();
      if (FIELD_RAW.equals(topLevelField)) {
        parser.nextToken();
        raw = parseStringOrMultipartStringObject();
        appendTokens(raw, null, positionIncrement);
        positionIncrement++;
      } else if (FIELD_REFS.equals(topLevelField)) {
        if (parser.nextToken() == JsonToken.START_OBJECT) {
          while (parser.nextToken() != JsonToken.END_OBJECT) {
            String referenceType = parser.getCurrentName();
            JsonToken t = parser.nextToken();
            if (t == JsonToken.START_ARRAY) {
              while (parser.nextToken() != JsonToken.END_ARRAY) {
                String payload = referenceType + PAYLOAD_ATTR_SEPARATOR + raw.toDelimitedString();
                appendTokens(parseStringOrMultipartStringObject(), payload, positionIncrement);
                positionIncrement++;
              }
            } else {
              throw new IOException("Expected start of array as object value for relationship = " + referenceType);
            }
          }
        } else {
          throw new IOException("Expected start of object as object value for " + FIELD_REFS);
        }
      }
    }
  }

  /**
   * Expects the current token from JSON parser to be either a string
   * or a JSON object representing a multipart string, and consumes it.
   */
  private MultiPartString parseStringOrMultipartStringObject() throws IOException {
    MultiPartString multiPartString = null;
    JsonToken t = parser.getCurrentToken();
    if(t == JsonToken.VALUE_STRING) {
      multiPartString = new MultiPartString(parser.getValueAsString());
    } else if(t == JsonToken.START_OBJECT) {
      String prefix = null, filing = null;
      while (parser.nextToken() != JsonToken.END_OBJECT) {
        String stringComponentType = parser.getCurrentName();
        parser.nextToken();
        String stringComponentValue = parser.getValueAsString();
        if (MULTIPART_STRING_PREFIX.equals(stringComponentType)) {
          prefix = stringComponentValue;
        } else if (MULTIPART_STRING_FILING.equals(stringComponentType)) {
          filing = stringComponentValue;
        } else {
          throw new IOException("Expected object key for multipart string (" + MULTIPART_STRING_PREFIX + ", " + MULTIPART_STRING_FILING + ") but got = " + stringComponentType);
        }
      }
      multiPartString = new MultiPartString(filing, prefix);
    } else {
      throw new IOException("Expected string or object representing multipart string, but got " + t.name());
    }
    multiPartString.setNormalized(normalize(multiPartString));
    return multiPartString;
  }

  private void setAttributesForToken(Token token) {
    termAtt.append(token.term);
    typeAtt.setType(token.type);
    posIncrAtt.setPositionIncrement(token.positionIncrement);
    //refAtt.setReferenceType(reference.referenceType);
    //refAtt.setTarget(raw);
    if(token.payload != null) {
      payloadAtt.setPayload(new BytesRef(token.payload));
    }
  }

  /**
   * This token stream includes both the 'raw' value and the 'target' values,
   * since we want to index both.
   *
   * @throws IOException
   */
  @Override
  public boolean incrementToken() throws IOException {
    clearAttributes();

    if (!consumed) {
      parser = jsonFactory.createParser(input);
      parse();
      parser.close();

      tokensIter = tokens.iterator();

      consumed = true;
    }

    if (tokensIter.hasNext()) {
      Token token = tokensIter.next();
      setAttributesForToken(token);
      return true;
    }

    return false;
  }

}
