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
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;

import java.io.IOException;
import java.util.Collections;
import static junit.framework.Assert.assertTrue;
import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.PayloadAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.analysis.tokenattributes.TypeAttribute;
import org.apache.lucene.util.BytesRef;

public class TokenTypeJoinFilterTest extends BaseTokenStreamTestCase {


  public void test() throws IOException {
    String test = "The quick red fox jumped over the lazy brown dogs";

    TokenTypeSplitFilter ttsf = new TokenTypeSplitFilter(new Blah(whitespaceMockTokenizer(test)), Collections.singleton("even"),
        Collections.EMPTY_SET, "even_fork", "even_orig");
    TokenTypeSplitFilter ttsfOdd = new TokenTypeSplitFilter(ttsf, Collections.singleton("odd"),
        Collections.EMPTY_SET, "odd_fork", "odd_orig");
    TokenTypeJoinFilter ttjf = new TokenTypeJoinFilter(ttsfOdd, new String[] {"even_orig", "even_fork"}, "joined", null, "!", false, true);
    int count = 0;
    TypeAttribute typeAtt = ttjf.getAttribute(TypeAttribute.class);
    OffsetAttribute offsetAtt = ttjf.getAttribute(OffsetAttribute.class);
    PositionIncrementAttribute posIncrAtt = ttjf.getAttribute(PositionIncrementAttribute.class);
    CharTermAttribute termAtt = ttjf.getAttribute(CharTermAttribute.class);
    PayloadAttribute payloadAtt = ttjf.getAttribute(PayloadAttribute.class);
    String lastTerm = null;
    int lastStartOffset = -1;
    int lastEndOffset = -1;
    ttjf.reset();
    while (ttjf.incrementToken()) {
      String term = termAtt.toString();
      String type = typeAtt.type();
      int startOffset = offsetAtt.startOffset();
      int endOffset = offsetAtt.endOffset();
      int posIncr = posIncrAtt.getPositionIncrement();
      BytesRef payload = payloadAtt.getPayload();
      switch (count % 3) {
        case 0:
          assertEquals("joined", type);
          assertEquals(1, posIncr);
          assertEquals(lastEndOffset + 1, startOffset);
          String[] split = term.split("!");
          assertEquals(split[0], split[1]);
          assertNull(payload);
          break;
        case 1:
          assertEquals("odd_orig", type);
          assertEquals(1, posIncr);
          assertEquals(lastEndOffset + 1, startOffset);
          assertNull(payload);
          break;
        case 2:
          assertEquals("odd_fork", type);
          assertEquals(lastTerm, term);
          assertEquals(0, posIncr);
          assertEquals(lastStartOffset, startOffset);
          assertEquals(lastEndOffset, endOffset);
          assertNull(payload);
          break;
      }
      lastTerm = term;
      lastStartOffset = startOffset;
      lastEndOffset = endOffset;
      count++;
    }
    assertTrue(count + " does not equal: " + 15, count == 15);

  }
  
  public void testOutputComponentTypes() throws IOException {
    String test = "The quick red fox jumped over the lazy brown dogs";

    TokenTypeSplitFilter ttsf = new TokenTypeSplitFilter(new Blah(whitespaceMockTokenizer(test)), Collections.singleton("even"),
        Collections.EMPTY_SET, "even_fork", "even_orig");
    TokenTypeSplitFilter ttsfOdd = new TokenTypeSplitFilter(ttsf, Collections.singleton("odd"),
        Collections.EMPTY_SET, "odd_fork", "odd_orig");
    TokenTypeJoinFilter ttjf = new TokenTypeJoinFilter(ttsfOdd, new String[] {"even_orig", "even_fork"}, "joined", null, "!", true, true);
    int count = 0;
    TypeAttribute typeAtt = ttjf.getAttribute(TypeAttribute.class);
    OffsetAttribute offsetAtt = ttjf.getAttribute(OffsetAttribute.class);
    PositionIncrementAttribute posIncrAtt = ttjf.getAttribute(PositionIncrementAttribute.class);
    CharTermAttribute termAtt = ttjf.getAttribute(CharTermAttribute.class);
    PayloadAttribute payloadAtt = ttjf.getAttribute(PayloadAttribute.class);
    String lastTerm = null;
    int lastStartOffset = -1;
    int lastEndOffset = -1;
    ttjf.reset();
    while (ttjf.incrementToken()) {
      String term = termAtt.toString();
      String type = typeAtt.type();
      int startOffset = offsetAtt.startOffset();
      int endOffset = offsetAtt.endOffset();
      int posIncr = posIncrAtt.getPositionIncrement();
      BytesRef payload = payloadAtt.getPayload();
      switch (count % 5) {
        case 0:
          assertEquals("even_orig", type);
          assertEquals(1, posIncr);
          assertEquals(lastEndOffset + 1, startOffset);
          assertNull(payload);
          break;
        case 1:
          assertEquals("even_fork", type);
          assertEquals(lastTerm, term);
          assertEquals(0, posIncr);
          assertEquals(lastStartOffset, startOffset);
          assertEquals(lastEndOffset, endOffset);
          assertNull(payload);
          break;
        case 2:
          assertEquals("joined", type);
          assertEquals(0, posIncr);
          assertEquals(lastStartOffset, startOffset);
          String[] split = term.split("!");
          assertEquals(split[0], split[1]);
          assertNull(payload);
          break;
        case 3:
          assertEquals("odd_orig", type);
          assertEquals(1, posIncr);
          assertEquals(lastEndOffset + 1, startOffset);
          assertNull(payload);
          break;
        case 4:
          assertEquals("odd_fork", type);
          assertEquals(lastTerm, term);
          assertEquals(0, posIncr);
          assertEquals(lastStartOffset, startOffset);
          assertEquals(lastEndOffset, endOffset);
          assertNull(payload);
          break;
      }
      lastTerm = term;
      lastStartOffset = startOffset;
      lastEndOffset = endOffset;
      count++;
    }
    assertTrue(count + " does not equal: " + 25, count == 25);

  }

  public void testVariableTokenPresence() throws IOException {
    String test = "The Quick Red Fox Jumped Over The Lazy Brown Dogs";
    TokenTypeJoinFilter ttjf = new TokenTypeJoinFilter(new Blah2(whitespaceMockTokenizer(test)), new String[] {"raw", "lower", "upper"}, 
        "joined", null, "!", false, false);
    CharTermAttribute termAtt = ttjf.getAttribute(CharTermAttribute.class);
    PayloadAttribute payloadAtt = ttjf.getAttribute(PayloadAttribute.class);
    ttjf.reset();
    int i = -1;
    String[] split = test.split(" ");
    StringBuilder sb = new StringBuilder();
    while (ttjf.incrementToken()) {
      String term = termAtt.toString();
      BytesRef payload = payloadAtt.getPayload();
      switch (++i) {
        case 0:
          assertEquals(split[i], term);
          assertNull(payload);
          break;
        case 1:
          sb.setLength(0);
          sb.append(split[i]).append('!').append(split[i].toUpperCase());
          assertEquals(sb.toString(), term);
          assertNull(payload);
          break;
        case 2:
          sb.setLength(0);
          sb.append(split[i]).append('!').append(split[i].toLowerCase()).append('!').append(split[i].toUpperCase());
          assertEquals(sb.toString(), term);
          assertNull(payload);
          break;
      }
    }
  }

  TokenArrayTokenizer.Token[] tokensWithPayloads = new TokenArrayTokenizer.Token[] {
          new TokenArrayTokenizer.Token("unconsoled", "normalized", 1, "payload1"),
          new TokenArrayTokenizer.Token("Unconsoled", "filing", 0, null),
          new TokenArrayTokenizer.Token("The ", "prefix", 0, null),

          new TokenArrayTokenizer.Token("room with a view", "normalized", 2, null),
          new TokenArrayTokenizer.Token("Room With A View", "filing", 0, "payload2"),
          new TokenArrayTokenizer.Token("A ", "prefix", 0, null),
  };

  /** verify that payload gets picked up for 1st group of tokens */
  public void testTypeForPayload1() throws IOException {
    TokenTypeJoinFilter ttjf = new TokenTypeJoinFilter(new TokenArrayTokenizer(tokensWithPayloads), new String[] {"normalized", "filing", "prefix"},
            "joined", "normalized", "!", false, false);
    CharTermAttribute termAtt = ttjf.getAttribute(CharTermAttribute.class);
    TypeAttribute typeAtt = ttjf.getAttribute(TypeAttribute.class);
    PayloadAttribute payloadAtt = ttjf.getAttribute(PayloadAttribute.class);
    ttjf.reset();

    assertTrue(ttjf.incrementToken());

    assertEquals("unconsoled!Unconsoled!The ", termAtt.toString());
    assertEquals("joined", typeAtt.type());
    assertEquals("payload1", payloadAtt.getPayload().utf8ToString());

    assertTrue(ttjf.incrementToken());

    assertEquals("room with a view!Room With A View!A ", termAtt.toString());
    assertEquals("joined", typeAtt.type());
    assertNull(payloadAtt.getPayload());

    assertFalse(ttjf.incrementToken());
  }

  /** verify that payload gets picked up for 2nd group of tokens */
  public void testTypeForPayload2() throws IOException {
    TokenTypeJoinFilter ttjf = new TokenTypeJoinFilter(new TokenArrayTokenizer(tokensWithPayloads), new String[] {"normalized", "filing", "prefix"},
            "joined", "filing", "!", false, false);
    CharTermAttribute termAtt = ttjf.getAttribute(CharTermAttribute.class);
    TypeAttribute typeAtt = ttjf.getAttribute(TypeAttribute.class);
    PayloadAttribute payloadAtt = ttjf.getAttribute(PayloadAttribute.class);
    ttjf.reset();

    assertTrue(ttjf.incrementToken());

    assertEquals("unconsoled!Unconsoled!The ", termAtt.toString());
    assertEquals("joined", typeAtt.type());
    assertNull(payloadAtt.getPayload());

    assertTrue(ttjf.incrementToken());

    assertEquals("room with a view!Room With A View!A ", termAtt.toString());
    assertEquals("joined", typeAtt.type());
    assertEquals("payload2", payloadAtt.getPayload().utf8ToString());

    assertFalse(ttjf.incrementToken());
  }

  /** verify that payload gets overwritten properly across groups of tokens*/
  public void testTypeForPayload3() throws IOException {
    TokenArrayTokenizer.Token[] tokensWithPayloads = new TokenArrayTokenizer.Token[] {
            new TokenArrayTokenizer.Token("unconsoled", "normalized", 1, null),
            new TokenArrayTokenizer.Token("Unconsoled", "filing", 0, "payload1"),
            new TokenArrayTokenizer.Token("The ", "prefix", 0, null),

            new TokenArrayTokenizer.Token("room with a view", "normalized", 2, null),
            new TokenArrayTokenizer.Token("Room With A View", "filing", 0, "payload2"),
            new TokenArrayTokenizer.Token("A ", "prefix", 0, null),

            new TokenArrayTokenizer.Token("no payload", "normalized", 3, null),
            new TokenArrayTokenizer.Token("No Payload", "filing", 0, null),
    };

    TokenTypeJoinFilter ttjf = new TokenTypeJoinFilter(new TokenArrayTokenizer(tokensWithPayloads), new String[] {"normalized", "filing", "prefix"},
            "joined", "filing", "!", false, false);
    CharTermAttribute termAtt = ttjf.getAttribute(CharTermAttribute.class);
    TypeAttribute typeAtt = ttjf.getAttribute(TypeAttribute.class);
    PayloadAttribute payloadAtt = ttjf.getAttribute(PayloadAttribute.class);
    ttjf.reset();

    assertTrue(ttjf.incrementToken());

    assertEquals("unconsoled!Unconsoled!The ", termAtt.toString());
    assertEquals("joined", typeAtt.type());
    assertEquals("payload1", payloadAtt.getPayload().utf8ToString());

    assertTrue(ttjf.incrementToken());

    assertEquals("room with a view!Room With A View!A ", termAtt.toString());
    assertEquals("joined", typeAtt.type());
    assertEquals("payload2", payloadAtt.getPayload().utf8ToString());

    assertTrue(ttjf.incrementToken());

    assertEquals("no payload!No Payload", termAtt.toString());
    assertEquals("joined", typeAtt.type());
    assertNull(payloadAtt.getPayload());

    assertFalse(ttjf.incrementToken());
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
        typeAtt.setType("odd");
      }
      return true;
    }

    @Override
    public void reset() throws IOException {
      super.reset();
      i = -1;
    }
    
  }

  private static final class Blah2 extends TokenFilter {

    private int i = -1;
    private int repeat = -1;
    private String rawTerm;
    private final TypeAttribute typeAtt = addAttribute(TypeAttribute.class);
    private final CharTermAttribute termAtt = addAttribute(CharTermAttribute.class);
    private final PositionIncrementAttribute posIncrAtt = addAttribute(PositionIncrementAttribute.class);

    public Blah2(TokenStream input) {
      super(input);
    }

    @Override
    public boolean incrementToken() throws IOException {
      switch (repeat--) {
        case 2:
          termAtt.setEmpty().append(rawTerm.toLowerCase());
          typeAtt.setType("lower");
          posIncrAtt.setPositionIncrement(0);
          break;
        case 1:
          termAtt.setEmpty().append(rawTerm.toUpperCase());
          typeAtt.setType("upper");
          posIncrAtt.setPositionIncrement(0);
          break;
        default:
          if (!this.input.incrementToken()) {
            return false;
          }
          rawTerm = termAtt.toString();
          typeAtt.setType("raw");
          repeat = ++i % 3;
          break;
      }
      return true;
    }

    @Override
    public void reset() throws IOException {
      super.reset();
      i = -1;
      repeat = -1;
      rawTerm = null;
    }

  }

  /** Tokenizer that simply emits the tokens passed into the constructor */
  public static final class TokenArrayTokenizer extends Tokenizer {
    private final TypeAttribute typeAtt = addAttribute(TypeAttribute.class);
    private final CharTermAttribute termAtt = addAttribute(CharTermAttribute.class);
    private final PositionIncrementAttribute posIncrAtt = addAttribute(PositionIncrementAttribute.class);
    private final PayloadAttribute payloadAtt = addAttribute(PayloadAttribute.class);
    private int i = 0;
    private Token[] tokens;

    public static class Token {
      String term;
      String type;
      int positionIncrement;
      String payload;
      public Token(String term, String type, int positionIncrement, String payload) {
        this.term = term;
        this.type = type;
        this.positionIncrement = positionIncrement;
        this.payload = payload;
      }
    }

    public TokenArrayTokenizer(Token[] tokens) {
      this.tokens = tokens;
    }

    @Override
    public boolean incrementToken() throws IOException {
      clearAttributes();
      if(i < tokens.length) {
        Token t = tokens[i];
        i++;
        typeAtt.setType(t.type);
        termAtt.append(t.term);
        posIncrAtt.setPositionIncrement(t.positionIncrement);
        if(t.payload != null) {
          payloadAtt.setPayload(new BytesRef(t.payload));
        }
        return true;
      }
      return false;
    }
  }

}