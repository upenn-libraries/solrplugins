package edu.upenn.library.solrplugins;

import java.util.Map;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.util.TokenizerFactory;
import org.apache.lucene.util.AttributeFactory;

/**
 * Accompanying factory for JsonReferencePayloadTokenizer.
 *
 * @author jeffchiu
 */
public class JsonReferencePayloadTokenizerFactory extends TokenizerFactory {

  public JsonReferencePayloadTokenizerFactory(Map<String,String> args) {
    super(args);
  }

  @Override
  public Tokenizer create(AttributeFactory factory) {
    return new JsonReferencePayloadTokenizer(factory);
  }

}
