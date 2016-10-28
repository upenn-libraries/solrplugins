package edu.upenn.library.solrplugins;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertNull;
import org.junit.Test;

public class MultiPartStringTest {

  @Test
  public void testParseNormalizedFilingAndPrefix() {
    MultiPartString result = MultiPartString.parseNormalizedFilingAndPrefix("norm" + MultiPartString.DELIMITER + "filing");
    assertEquals("norm", result.getNormalized());
    assertEquals("filing", result.getFiling());
    assertNull(result.getPrefix());

    result = MultiPartString.parseNormalizedFilingAndPrefix(MultiPartString.DELIMITER + "filing");
    assertEquals("", result.getNormalized());
    assertEquals("filing", result.getFiling());
    assertNull(result.getPrefix());

    result = MultiPartString.parseNormalizedFilingAndPrefix(MultiPartString.DELIMITER);
    assertEquals("", result.getNormalized());
    assertEquals("", result.getFiling());
    assertNull(result.getPrefix());

    // with prefix

    result = MultiPartString.parseNormalizedFilingAndPrefix("norm" + MultiPartString.DELIMITER + "filing" + MultiPartString.DELIMITER + "prefix");
    assertEquals("norm", result.getNormalized());
    assertEquals("filing", result.getFiling());
    assertEquals("prefix", result.getPrefix());

    result = MultiPartString.parseNormalizedFilingAndPrefix(MultiPartString.DELIMITER + "filing" + MultiPartString.DELIMITER + "prefix");
    assertEquals("", result.getNormalized());
    assertEquals("filing", result.getFiling());
    assertEquals("prefix", result.getPrefix());

    result = MultiPartString.parseNormalizedFilingAndPrefix(MultiPartString.DELIMITER + MultiPartString.DELIMITER);
    assertEquals("", result.getNormalized());
    assertEquals("", result.getFiling());
    assertEquals("", result.getPrefix());

    // zero-length prefix (has delimiter)

    result = MultiPartString.parseNormalizedFilingAndPrefix("norm" + MultiPartString.DELIMITER + "filing" + MultiPartString.DELIMITER);
    assertEquals("norm", result.getNormalized());
    assertEquals("filing", result.getFiling());
    assertEquals("", result.getPrefix());

    result = MultiPartString.parseNormalizedFilingAndPrefix(MultiPartString.DELIMITER + "filing" + MultiPartString.DELIMITER);
    assertEquals("", result.getNormalized());
    assertEquals("filing", result.getFiling());
    assertEquals("", result.getPrefix());
  }

}
