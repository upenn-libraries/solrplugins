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
import org.apache.lucene.util.AttributeFactory;
import org.apache.lucene.util.BytesRef;

/**
 * Tokenizer that deserializes a JSON object from a string,
 * and tokenizes both its "raw" object field and the target values
 * in its "refs" nested object. The JSON object should have
 * the following structure:
 * {
 *   "raw": "Georg Wilhelm Friedrich Hegel",
 *   "refs": {
 *     "use_for": ["G. W. F. Hegel", "Hegel"],
 *     "see_also": ["Hegelianism"]
 *   }
 * }
 *
 * @author jeffchiu
 */
public final class JsonReferencePayloadTokenizer extends Tokenizer {

  private static final JsonFactory jsonFactory = new JsonFactory();
  public static final String PAYLOAD_ATTR_SEPARATOR = ":";
  private static final String FIELD_RAW = "raw";
  private static final String FIELD_REFS = "refs";

  private final CharTermAttribute termAtt = addAttribute(CharTermAttribute.class);
  // I couldn't get this custom ReferenceAttribute to work.
  //private final ReferenceAttribute refAtt = addAttribute(ReferenceAttribute.class);
  private final PayloadAttribute payloadAtt = addAttribute(PayloadAttribute.class);

  private boolean consumed = false;
  private JsonParser parser;
  private String raw;
  private List<Reference> references = new ArrayList<>();
  private Iterator<Reference> referencesIter;

  public class Reference {
    String referenceType;
    String target;
  }

  public JsonReferencePayloadTokenizer() {
    super();
  }

  public JsonReferencePayloadTokenizer(AttributeFactory factory) {
    super(factory);
  }

  private void parse() throws IOException {
    if (parser.nextToken() != JsonToken.START_OBJECT) {
      throw new IOException("Expected data to start with a START_OBJECT token, but found this instead: " + parser.getCurrentToken());
    }

    while (parser.nextToken() != JsonToken.END_OBJECT) {
      String topLevelField = parser.getCurrentName();
      if (FIELD_RAW.equals(topLevelField)) {
        parser.nextToken();
        raw = parser.getValueAsString();
      } else if (FIELD_REFS.equals(topLevelField)) {
        if (parser.nextToken() == JsonToken.START_OBJECT) {
          while (parser.nextToken() != JsonToken.END_OBJECT) {
            String referenceType = parser.getCurrentName();
            JsonToken t = parser.nextToken();
            if (t == JsonToken.START_ARRAY) {
              while (parser.nextToken() != JsonToken.END_ARRAY) {
                Reference ref = new Reference();
                ref.referenceType = referenceType;
                ref.target = parser.getValueAsString();
                references.add(ref);
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
   * This token stream includes both the 'raw' value and the 'target' values,
   * since we want to index both.
   *
   * @throws IOException
   */
  @Override
  public boolean incrementToken() throws IOException {
    if (!consumed) {
      clearAttributes();

      parser = jsonFactory.createParser(input);
      parse();
      parser.close();

      termAtt.append(raw);
      termAtt.setLength(raw.length());

      referencesIter = references.iterator();

      consumed = true;

      return true;
    }

    if (referencesIter.hasNext()) {
      Reference reference = referencesIter.next();
      termAtt.setEmpty();
      termAtt.append(reference.target);
      termAtt.setLength(reference.target.length());
      //refAtt.setReferenceType(reference.referenceType);
      //refAtt.setTarget(raw);
      // note inversion: "target" in attribute is the "raw" field value from JSON
      String s = reference.referenceType + PAYLOAD_ATTR_SEPARATOR + raw;
      payloadAtt.setPayload(new BytesRef(s));
      return true;
    }

    return false;
  }

}
