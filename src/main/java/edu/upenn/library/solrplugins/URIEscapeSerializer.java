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
