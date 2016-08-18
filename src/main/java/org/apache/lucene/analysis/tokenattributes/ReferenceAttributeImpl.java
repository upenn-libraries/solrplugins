package org.apache.lucene.analysis.tokenattributes;

import org.apache.lucene.util.AttributeImpl;
import org.apache.lucene.util.AttributeReflector;

/**
 *
 */
public class ReferenceAttributeImpl extends AttributeImpl implements ReferenceAttribute, Cloneable {

  public static final String PAYLOAD_ATTR_SEPARATOR = ":";

  private String referenceType = null;
  private String target = null;

  public void setTarget(String target) {
    this.target = target;
  }

  @Override
  public String getTarget() {
    return target;
  }

  public void setReferenceType(String referenceType) {
    this.target = referenceType;
  }

  @Override
  public String getReferenceType() {
    return referenceType;
  }

  @Override
  public void clear() {
    referenceType = null;
    target = null;
  }

  /**
   * null-safe string equality comparison
   */
  private static boolean strEquals(String s1, String s2) {
    if (s1 == null) {
      return s2 == null;
    }
    return s1.equals(s2);
  }

  @Override
  public boolean equals(Object other) {
    if (other == this) {
      return true;
    }

    if (other instanceof org.apache.lucene.analysis.tokenattributes.ReferenceAttributeImpl) {
      org.apache.lucene.analysis.tokenattributes.ReferenceAttributeImpl o = (org.apache.lucene.analysis.tokenattributes.ReferenceAttributeImpl) other;
      return strEquals(o.referenceType, referenceType) && strEquals(o.target, target);
    }

    return false;
  }

  @Override
  public int hashCode() {
    String s = referenceType + PAYLOAD_ATTR_SEPARATOR + target;
    return s.hashCode();
  }

  @Override
  public void copyTo(AttributeImpl targetAttr) {
    ReferenceAttribute t = (ReferenceAttribute) targetAttr;
    t.setReferenceType(referenceType);
    t.setTarget(target);
  }

  @Override
  public void reflectWith(AttributeReflector reflector) {
    reflector.reflect(ReferenceAttribute.class, "referenceType", referenceType);
    reflector.reflect(ReferenceAttribute.class, "target", target);
  }
}
