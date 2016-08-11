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
package edu.upenn.library.solrplugins.tokentype;

import java.util.Map;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.util.TokenFilterFactory;

/**
 * Joins overlapping tokens of the specified types, in the specified order, using the specified delimiter, assigning
 * specified type to combined output token.
 *
 * @author michael
 */
public class TokenTypeJoinFilterFactory extends TokenFilterFactory {

  private static final String INPUT_TYPES_ARGNAME = "inputTypes";
  private static final String DELIM_CODEPOINT_ARGNAME = "delimCodepoint";
  private static final String OUTPUT_TYPE_ARGNAME = "outputType";

  private static final char DEFAULT_DELIM = '\u0000';

  private final String[] inputTypes;
  private final String outputType;
  private final char delim;

  public TokenTypeJoinFilterFactory(Map<String, String> args) {
    super(args);
    delim = args.containsKey(DELIM_CODEPOINT_ARGNAME) ? Character.toChars(Integer.parseInt(args.get(DELIM_CODEPOINT_ARGNAME)))[0] : DEFAULT_DELIM;
    inputTypes = args.get(INPUT_TYPES_ARGNAME).split("\\s*,\\s*");
    outputType = args.get(OUTPUT_TYPE_ARGNAME);
  }

  @Override
  public TokenStream create(TokenStream input) {
    return new TokenTypeJoinFilter(input, inputTypes, outputType, delim);
  }

}
