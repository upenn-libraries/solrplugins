package edu.upenn.library.solrplugins.callno;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 *
 * @author michael
 */
public class Padder {

    private final String[] pads;
    private final boolean lexicographic;

    public Padder(StringBuilder sb, int limit, char pad, boolean lexicographic) {
        pads = generatePads(sb, limit, pad);
        this.lexicographic = lexicographic;
    }

    private static String[] generatePads(StringBuilder sb, int limit, char pad) {
        sb.setLength(0);
        String[] ret = new String[limit + 1];
        ret[ret.length - 1] = "";
        for (int i = ret.length - 2; i >= 0; i--) {
            sb.append(pad);
            ret[i] = sb.toString().intern();
        }
        return ret;
    }

    /**
     * Appends s, no null-checking or padding, updates groupIndexes.  
     * Will throw NPE if s is null
     * @param sb
     * @param s
     * @param groupIndexes
     * @param index 
     */
    public static void appendStrict(StringBuilder sb, String s, int[] groupIndexes, int index) {
        sb.append(s);
        groupIndexes[index] = sb.length();
    }
    
    /**
     * Appends c, updates groupIndexes.
     * @param sb
     * @param c
     * @param groupIndexes
     * @param index 
     */
    public static void appendStrict(StringBuilder sb, char c, int[] groupIndexes, int index) {
        sb.append(c);
        groupIndexes[index] = sb.length();
    }
    
    /**
     * Appends s, no padding, returns false if s is null
     * @param sb
     * @param s
     * @param groupIndexes
     * @param index
     * @return 
     */
    public static boolean appendDirect(StringBuilder sb, String s, int[] groupIndexes, int index) {
        if (s == null || s.length() == 0) {
            return false;
        } else {
            sb.append(s);
            groupIndexes[index] = sb.length();
            return true;
        }
    }
    
    /**
     * Appends s, with padding, will throw ArrayIndexOutOfBoundsException if 
     * s is longer than the length limit of this Padder
     * @param sb
     * @param s
     * @param groupIndexes
     * @param index
     * @return 
     */
    public boolean appendPadded(StringBuilder sb, String s, int[] groupIndexes, int index) {
        if (appendPadded(sb, s)) {
            groupIndexes[index] = sb.length();
            return true;
        } else {
            return false;
        }
    }
    
    public boolean appendPadded(StringBuilder sb, String s) {
        int len;
        if (s == null || (len = s.length()) == 0) {
            sb.append(pads[0]);
            return false;
        } else {
            if (lexicographic) {
                sb.append(s);
                sb.append(pads[len]);
            } else {
                sb.append(pads[len]);
                sb.append(s);
            }
            return true;
        }
    }

    public boolean appendVariable(StringBuilder sb, String s, int[] groupIndexes, int index, Pattern p, String replacement) {
        if (s == null) {
            sb.append(pads[0]);
            return false;
        } else {
            int alphaLimit = pads.length - 1;
            CharSequence collapsed;
            if (p == null) {
                collapsed = s;
            } else {
                Matcher m = p.matcher(s);
                if (m.find()) {
                    boolean remaining = true;
                    StringBuffer buf = new StringBuffer(s.length());
                    do {
                        m.appendReplacement(buf, replacement);
                    } while ((remaining = m.find()) && buf.length() < alphaLimit);
                    if (!remaining) {
                        m.appendTail(buf);
                    }
                    if (buf.length() > alphaLimit) {
                        buf.setLength(alphaLimit);
                    }
                    collapsed = buf;
                } else {
                    collapsed = s;
                }
            }
            int len = collapsed.length();
            if (len > alphaLimit) {
                sb.append(collapsed, 0, alphaLimit);
            } else if (len > 0) {
                if (lexicographic) {
                    sb.append(collapsed);
                    sb.append(pads[len]);
                } else {
                    sb.append(pads[len]);
                    sb.append(collapsed);
                }
            } else {
                sb.append(pads[0]);
                return false;
            }
            groupIndexes[index] = sb.length();
            return true;
        }
    }
}
