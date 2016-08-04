package edu.upenn.library.solrplugins;

import org.apache.lucene.util.CharsRef;

/**
 *
 * @author michael
 */
public class FilingPrefixIgnorer implements TextTransformer {

  @Override
  public CharsRef transform(CharsRef input) {
    return new CharsRef(transform(input.toString()));
  }

  @Override
  public String transform(String input) {
    int i = input.indexOf('\t');
    return i < 0 ? input : input.substring(i + 1);
  }

}
