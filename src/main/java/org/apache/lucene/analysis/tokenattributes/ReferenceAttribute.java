package org.apache.lucene.analysis.tokenattributes;

import org.apache.lucene.util.Attribute;

/**
 *
 */
public interface ReferenceAttribute extends Attribute {

  public void setTarget(String target);

  public String getTarget();

  public void setReferenceType(String referenceType);

  public String getReferenceType();
}
