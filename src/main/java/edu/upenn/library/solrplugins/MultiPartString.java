package edu.upenn.library.solrplugins;

/**
 * Strings of form:
 *
 *   normalized + DELIMITER + filing + DELIMITER + prefix
 *
 * This makes sorting on the normalized form possible,
 * while preserving the original string (which is the prefix + filing).
 * Note that the last pair of DELIMITER + prefix is optional.
 *
 * @author jeffchiu
 */
public class MultiPartString {
  public static final String DELIMITER = "\u0000";
  private String normalized;
  private String filing;
  private String prefix;

  public MultiPartString(String normalized, String filing, String prefix) {
    this.normalized = normalized;
    this.filing = filing;
    this.prefix = prefix;
  }

  public MultiPartString(String filing, String prefix) {
    this.filing = filing;
    this.prefix = prefix;
  }

  public MultiPartString(String filing) {
    this.filing = filing;
  }

  public String getNormalized() {
    return normalized;
  }

  public void setNormalized(String normalized) {
    this.normalized = normalized;
  }

  public String getFiling() {
    return filing;
  }

  public String getPrefix() {
    return prefix;
  }

  public String getDisplay() {
    return (prefix != null ? prefix : "") + (filing != null ? filing : "");
  }

  /**
   * prefix is optionally present.
   * This exists to parse indexed term values, which begin with the normalized form.
   */
  public static MultiPartString parseNormalizedFilingAndPrefix(String s) {
    String[] parts = s.split(DELIMITER, -1);
    String normalized = null;
    String filing = null;
    if(parts.length > 0) {
      normalized = parts[0];
      filing = parts[1];
    }
    String prefix = null;
    if(parts.length > 2) {
      prefix = parts[2];
    }
    return new MultiPartString(normalized, filing, prefix);
  }

  /**
   * prefix is optionally present.
   * This exists to parse payload attribute values, which don't store the normalized form.
   */
  public static MultiPartString parseFilingAndPrefix(String s) {
    String[] parts = s.split(DELIMITER, -1);
    String filing = parts[0];
    String prefix = null;
    if(parts.length > 1) {
      prefix = parts[1];
    }
    return new MultiPartString(filing, prefix);
  }

  /**
   * prefix is only included if present
   */
  public String toDelimitedStringForFilingAndPrefix() {
    StringBuilder b = new StringBuilder();
    b.append(filing);
    if(prefix != null) {
      b.append(DELIMITER);
      b.append(prefix);
    }
    return b.toString();
  }

}
