package edu.upenn.library.solrplugins;

import org.apache.lucene.util.CharsRef;

/**
 *
 * @author michael
 */
public class NonFilingPrefixCombiner implements TextTransformer {

  @Override
  public CharsRef transform(CharsRef input) {
    return new CharsRef(transform(input.toString()));
  }

  @Override
  public String transform(String input) {
    return input.replaceFirst("\t", "");
  }

}
