package edu.upenn.library.solrplugins;

import java.io.IOException;
import java.io.StringReader;
import static junit.framework.Assert.assertEquals;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.PayloadAttribute;
import static org.junit.Assert.assertFalse;
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

    assertTrue(tokenizer.incrementToken());
    assertEquals("ref1", tokenizer.getAttribute(CharTermAttribute.class).toString());
    assertEquals("use_for" + JsonReferencePayloadTokenizer.PAYLOAD_ATTR_SEPARATOR + "some value", tokenizer.getAttribute(PayloadAttribute.class).getPayload().utf8ToString());

    assertTrue(tokenizer.incrementToken());
    assertEquals("ref2", tokenizer.getAttribute(CharTermAttribute.class).toString());
    assertEquals("use_for" + JsonReferencePayloadTokenizer.PAYLOAD_ATTR_SEPARATOR + "some value", tokenizer.getAttribute(PayloadAttribute.class).getPayload().utf8ToString());

    assertTrue(tokenizer.incrementToken());
    assertEquals("ref3", tokenizer.getAttribute(CharTermAttribute.class).toString());
    assertEquals("see_also" + JsonReferencePayloadTokenizer.PAYLOAD_ATTR_SEPARATOR + "some value", tokenizer.getAttribute(PayloadAttribute.class).getPayload().utf8ToString());

    assertFalse(tokenizer.incrementToken());
  }

}
