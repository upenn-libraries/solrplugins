package edu.upenn.library.solrplugins;

import org.apache.lucene.util.CharsRef;

/**
 *
 * @author michael
 */
public interface TextTransformer {

  CharsRef transform(CharsRef input);

  String transform(String input);

}
