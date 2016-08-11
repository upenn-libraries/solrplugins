/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.upenn.library.solrplugins.tokentype;

import org.apache.lucene.analysis.BaseTokenStreamTestCase;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;

import java.io.IOException;
import java.util.Collections;
import static junit.framework.Assert.assertTrue;
import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.core.UpperCaseFilter;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.analysis.tokenattributes.TypeAttribute;

public class TokenTypeProcessFilterTest extends BaseTokenStreamTestCase {

  public void test() throws IOException {
    String test = "The quick red fox jumped over the lazy brown dogs";

    TokenTypeProcessFilter ttpf = new TokenTypeProcessFilter(new Blah(whitespaceMockTokenizer(test)), Collections.singleton("even"),
        Collections.EMPTY_SET, "even_processed");
    ttpf.setDelegate(new UpperCaseFilter(ttpf));
    int count = 0;
    TypeAttribute typeAtt = ttpf.getAttribute(TypeAttribute.class);
    OffsetAttribute offsetAtt = ttpf.getAttribute(OffsetAttribute.class);
    PositionIncrementAttribute posIncrAtt = ttpf.getAttribute(PositionIncrementAttribute.class);
    CharTermAttribute termAtt = ttpf.getAttribute(CharTermAttribute.class);
    int lastEndOffset = -1;
    ttpf.reset();
    while (ttpf.incrementToken()) {
      String term = termAtt.toString();
      String type = typeAtt.type();
      int startOffset = offsetAtt.startOffset();
      int endOffset = offsetAtt.endOffset();
      int posIncr = posIncrAtt.getPositionIncrement();
      assertEquals(1, posIncr);
      assertEquals(lastEndOffset + 1, startOffset);
      switch (count % 2) {
        case 0:
          assertEquals(term.toUpperCase(), term);
          assertEquals("even_processed", type);
          break;
        case 1:
          assertTrue(!term.equals(term.toUpperCase()));
          break;
      }
      lastEndOffset = endOffset;
      count++;
    }
    assertTrue(count + " does not equal: " + 10, count == 10);

  }
  
  private static final class Blah extends TokenFilter {

    private int i = -1;
    private final TypeAttribute typeAtt = addAttribute(TypeAttribute.class);

    public Blah(TokenStream input) {
      super(input);
    }
    
    @Override
    public boolean incrementToken() throws IOException {
      if (!this.input.incrementToken()) {
        return false;
      }
      if (++i % 2 == 0) {
        typeAtt.setType("even");
      } else {
        typeAtt.setType(null);
      }
      return true;
    }

    @Override
    public void reset() throws IOException {
      super.reset();
      i = -1;
    }
    
  }


}