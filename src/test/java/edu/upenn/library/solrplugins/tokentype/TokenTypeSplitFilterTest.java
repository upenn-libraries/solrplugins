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
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.analysis.tokenattributes.TypeAttribute;

public class TokenTypeSplitFilterTest extends BaseTokenStreamTestCase {


  public void test() throws IOException {
    String test = "The quick red fox jumped over the lazy brown dogs";

    TokenTypeSplitFilter ttsf = new TokenTypeSplitFilter(new Blah(whitespaceMockTokenizer(test)), Collections.singleton("even"),
        Collections.EMPTY_SET, "even_fork", "even_orig");
    int count = 0;
    TypeAttribute typeAtt = ttsf.getAttribute(TypeAttribute.class);
    OffsetAttribute offsetAtt = ttsf.getAttribute(OffsetAttribute.class);
    PositionIncrementAttribute posIncrAtt = ttsf.getAttribute(PositionIncrementAttribute.class);
    CharTermAttribute termAtt = ttsf.getAttribute(CharTermAttribute.class);
    String lastTerm = null;
    int lastStartOffset = -1;
    int lastEndOffset = -1;
    ttsf.reset();
    while (ttsf.incrementToken()) {
      String term = termAtt.toString();
      String type = typeAtt.type();
      int startOffset = offsetAtt.startOffset();
      int endOffset = offsetAtt.endOffset();
      int posIncr = posIncrAtt.getPositionIncrement();
      switch (count % 3) {
        case 0:
          assertEquals("even_orig", type);
          assertEquals(1, posIncr);
          assertEquals(lastEndOffset + 1, startOffset);
          break;
        case 1:
          assertEquals("even_fork", type);
          assertEquals(lastTerm, term);
          assertEquals(0, posIncr);
          assertEquals(lastStartOffset, startOffset);
          assertEquals(lastEndOffset, endOffset);
          break;
        case 2:
          assertEquals(null, type);
          assertEquals(1, posIncr);
          assertEquals(lastEndOffset + 1, startOffset);
          break;
      }
      lastTerm = term;
      lastStartOffset = startOffset;
      lastEndOffset = endOffset;
      count++;
    }
    assertTrue(count + " does not equal: " + 15, count == 15);

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