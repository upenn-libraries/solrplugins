package edu.upenn.library.solrplugins;

import java.io.IOException;
import java.io.StringReader;
import static junit.framework.Assert.assertEquals;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.PayloadAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.analysis.tokenattributes.TypeAttribute;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import org.junit.Test;

public class JsonReferencePayloadTokenizerTest {

  @Test
  public void testBasic() throws IOException {
    JsonReferencePayloadTokenizer tokenizer = new JsonReferencePayloadTokenizer();
    tokenizer.setReader(new StringReader("{\"raw\": \"some value\", \"refs\": {\"use_for\":[\"ref1\",\"ref2\"], \"see_also\":[\"ref3\"]}}"));
    tokenizer.reset();

    assertTrue(tokenizer.incrementToken());
    assertEquals("some value", tokenizer.getAttribute(CharTermAttribute.class).toString());
    assertEquals(JsonReferencePayloadTokenizer.TYPE_FILING, tokenizer.getAttribute(TypeAttribute.class).type());
    assertEquals(1, tokenizer.getAttribute(PositionIncrementAttribute.class).getPositionIncrement());
    assertNull(tokenizer.getAttribute(PayloadAttribute.class).getPayload());

    assertTrue(tokenizer.incrementToken());
    assertEquals("ref1", tokenizer.getAttribute(CharTermAttribute.class).toString());
    assertEquals(JsonReferencePayloadTokenizer.TYPE_FILING, tokenizer.getAttribute(TypeAttribute.class).type());
    assertEquals(2, tokenizer.getAttribute(PositionIncrementAttribute.class).getPositionIncrement());
    assertEquals("use_for" + JsonReferencePayloadTokenizer.PAYLOAD_ATTR_SEPARATOR + "some value", tokenizer.getAttribute(PayloadAttribute.class).getPayload().utf8ToString());

    assertTrue(tokenizer.incrementToken());
    assertEquals("ref2", tokenizer.getAttribute(CharTermAttribute.class).toString());
    assertEquals(JsonReferencePayloadTokenizer.TYPE_FILING, tokenizer.getAttribute(TypeAttribute.class).type());
    assertEquals(3, tokenizer.getAttribute(PositionIncrementAttribute.class).getPositionIncrement());
    assertEquals("use_for" + JsonReferencePayloadTokenizer.PAYLOAD_ATTR_SEPARATOR + "some value", tokenizer.getAttribute(PayloadAttribute.class).getPayload().utf8ToString());

    assertTrue(tokenizer.incrementToken());
    assertEquals("ref3", tokenizer.getAttribute(CharTermAttribute.class).toString());
    assertEquals(JsonReferencePayloadTokenizer.TYPE_FILING, tokenizer.getAttribute(TypeAttribute.class).type());
    assertEquals(4, tokenizer.getAttribute(PositionIncrementAttribute.class).getPositionIncrement());
    assertEquals("see_also" + JsonReferencePayloadTokenizer.PAYLOAD_ATTR_SEPARATOR + "some value", tokenizer.getAttribute(PayloadAttribute.class).getPayload().utf8ToString());

    assertFalse(tokenizer.incrementToken());
  }

  @Test
  public void testMultipartStrings() throws IOException {
    JsonReferencePayloadTokenizer tokenizer = new JsonReferencePayloadTokenizer();
    tokenizer.setReader(new StringReader("{\"raw\": {\"prefix\": \"the \", \"filing\": \"unconsoled\"}, \"refs\": {\"use_for\":[\"ref1\",{\"prefix\": \"a \", \"filing\": \"chicken\"}], \"see_also\":[\"ref3\"]}}"));
    tokenizer.reset();

    assertTrue(tokenizer.incrementToken());
    assertEquals("unconsoled", tokenizer.getAttribute(CharTermAttribute.class).toString());
    assertEquals(JsonReferencePayloadTokenizer.TYPE_FILING, tokenizer.getAttribute(TypeAttribute.class).type());
    assertEquals(1, tokenizer.getAttribute(PositionIncrementAttribute.class).getPositionIncrement());
    assertNull(tokenizer.getAttribute(PayloadAttribute.class).getPayload());

    assertTrue(tokenizer.incrementToken());
    assertEquals("the ", tokenizer.getAttribute(CharTermAttribute.class).toString());
    assertEquals(JsonReferencePayloadTokenizer.TYPE_PREFIX, tokenizer.getAttribute(TypeAttribute.class).type());
    assertEquals(0, tokenizer.getAttribute(PositionIncrementAttribute.class).getPositionIncrement());
    assertNull(tokenizer.getAttribute(PayloadAttribute.class).getPayload());

    assertTrue(tokenizer.incrementToken());
    assertEquals("ref1", tokenizer.getAttribute(CharTermAttribute.class).toString());
    assertEquals(JsonReferencePayloadTokenizer.TYPE_FILING, tokenizer.getAttribute(TypeAttribute.class).type());
    assertEquals(2, tokenizer.getAttribute(PositionIncrementAttribute.class).getPositionIncrement());
    assertEquals("use_for" + JsonReferencePayloadTokenizer.PAYLOAD_ATTR_SEPARATOR + "unconsoled" + MultiPartString.DELIMITER + "the ", tokenizer.getAttribute(PayloadAttribute.class).getPayload().utf8ToString());

    assertTrue(tokenizer.incrementToken());
    assertEquals("chicken", tokenizer.getAttribute(CharTermAttribute.class).toString());
    assertEquals(JsonReferencePayloadTokenizer.TYPE_FILING, tokenizer.getAttribute(TypeAttribute.class).type());
    assertEquals(3, tokenizer.getAttribute(PositionIncrementAttribute.class).getPositionIncrement());
    assertEquals("use_for" + JsonReferencePayloadTokenizer.PAYLOAD_ATTR_SEPARATOR + "unconsoled" + MultiPartString.DELIMITER + "the ", tokenizer.getAttribute(PayloadAttribute.class).getPayload().utf8ToString());

    assertTrue(tokenizer.incrementToken());
    assertEquals("a ", tokenizer.getAttribute(CharTermAttribute.class).toString());
    assertEquals(JsonReferencePayloadTokenizer.TYPE_PREFIX, tokenizer.getAttribute(TypeAttribute.class).type());
    assertEquals(0, tokenizer.getAttribute(PositionIncrementAttribute.class).getPositionIncrement());
    assertEquals("use_for" + JsonReferencePayloadTokenizer.PAYLOAD_ATTR_SEPARATOR + "unconsoled" + MultiPartString.DELIMITER + "the ", tokenizer.getAttribute(PayloadAttribute.class).getPayload().utf8ToString());

    assertTrue(tokenizer.incrementToken());
    assertEquals("ref3", tokenizer.getAttribute(CharTermAttribute.class).toString());
    assertEquals(JsonReferencePayloadTokenizer.TYPE_FILING, tokenizer.getAttribute(TypeAttribute.class).type());
    assertEquals(4, tokenizer.getAttribute(PositionIncrementAttribute.class).getPositionIncrement());
    assertEquals("see_also" + JsonReferencePayloadTokenizer.PAYLOAD_ATTR_SEPARATOR + "unconsoled" + MultiPartString.DELIMITER + "the ", tokenizer.getAttribute(PayloadAttribute.class).getPayload().utf8ToString());

    assertFalse(tokenizer.incrementToken());
  }

}
