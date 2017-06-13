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

import java.util.Arrays;
import java.util.HashMap;
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
  private static final String HIERARCHY_LEVEL_ARGNAME = "hierarchyLevel";
  private static final String OUTPUT_TYPE_ARGNAME = "outputType";
  private static final String TYPE_FOR_PAYLOAD_ARGNAME = "typeForPayload";
  private static final String DISPLAY_COMPONENT_TYPES_ARGNAME = "displayComponentTypes";
  private static final String OUTPUT_COMPONENTS_ARGNAME = "outputComponents";
  private static final String APPEND_PLACEHOLDERS_ARGNAME = "appendPlaceholders";
  private static final boolean DEFAULT_OUTPUT_COMPONENTS = false;
  private static final boolean DEFAULT_APPEND_PLACEHOLDERS = false;
  private static final int DEFAULT_HIERARCHY_LEVEL = 0;

  private static final char DEFAULT_DELIM = '\u0000';

  private final Map<String, Integer> inputTypes;
  private final String outputType;
  private final int[] displayComponentTypes;
  private final String typeForPayload;
  private final String delim;
  private final boolean outputComponents;
  private final boolean appendPlaceholders;

  public TokenTypeJoinFilterFactory(Map<String, String> args) {
    super(args);
    char delimChar = args.containsKey(DELIM_CODEPOINT_ARGNAME) ? Character.toChars(Integer.parseInt(args.get(DELIM_CODEPOINT_ARGNAME)))[0] : DEFAULT_DELIM;
    int hierarchyLevel = args.containsKey(HIERARCHY_LEVEL_ARGNAME) ? Integer.parseInt(args.get(HIERARCHY_LEVEL_ARGNAME)) : DEFAULT_HIERARCHY_LEVEL;
    if (hierarchyLevel <= 0) {
      delim = Character.toString(delimChar);
    } else {
      char[] delimBuilder = new char[hierarchyLevel + 1];
      Arrays.fill(delimBuilder, delimChar);
      delim = new String(delimBuilder);
    }
    String[] inputTypesArr = args.get(INPUT_TYPES_ARGNAME).split("\\s*,\\s*");
    inputTypes = new HashMap<>(inputTypesArr.length * 2);
    for (int i = 0; i < inputTypesArr.length; i++) {
      inputTypes.put(inputTypesArr[i], i);
    }
    String displayComponentsStr = args.get(DISPLAY_COMPONENT_TYPES_ARGNAME);
    if (displayComponentsStr == null) {
      displayComponentTypes = null;
    } else {
      String[] displayComponentTypesArr = args.get(DISPLAY_COMPONENT_TYPES_ARGNAME).split("\\s*,\\s*");
      displayComponentTypes = new int[displayComponentTypesArr.length];
      for (int i = 0; i < displayComponentTypes.length; i++) {
        displayComponentTypes[i] = inputTypes.get(displayComponentTypesArr[i]);
      }
    }
    outputType = args.get(OUTPUT_TYPE_ARGNAME);
    typeForPayload = args.get(TYPE_FOR_PAYLOAD_ARGNAME);
    String outputComponentsS = args.get(OUTPUT_COMPONENTS_ARGNAME);
    this.outputComponents = outputComponentsS == null ? DEFAULT_OUTPUT_COMPONENTS : Boolean.parseBoolean(outputComponentsS);
    String appendPlaceholdersS = args.get(APPEND_PLACEHOLDERS_ARGNAME);
    this.appendPlaceholders = appendPlaceholdersS == null ? DEFAULT_APPEND_PLACEHOLDERS : Boolean.parseBoolean(appendPlaceholdersS);
  }

  @Override
  public TokenStream create(TokenStream input) {
    return new TokenTypeJoinFilter(input, inputTypes, outputType, typeForPayload, delim, outputComponents, appendPlaceholders, displayComponentTypes);
  }

}
