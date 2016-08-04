package edu.upenn.library.solrplugins;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import org.apache.lucene.util.CharsRef;

/**
 *
 * @author michael
 */
public class URIEscapeSerializer implements TextTransformer {

  @Override
  public CharsRef transform(CharsRef input) {
    return new CharsRef(transform(input.toString()));
  }

  @Override
  public String transform(String input) {
    try {
      return URLEncoder.encode(input, "UTF-8");
    } catch (UnsupportedEncodingException ex) {
      throw new RuntimeException(ex);
    }
  }

}
