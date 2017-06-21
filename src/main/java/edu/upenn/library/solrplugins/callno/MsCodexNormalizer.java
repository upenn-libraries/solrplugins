package edu.upenn.library.solrplugins.callno;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 *
 * @author michael
 */
public class MsCodexNormalizer implements Normalizer {

    static final int alphaLimit = 3;
    static final int numLimit = 5;
    static final String msCodexPattern = "^\\s*m[^a-z0-9]*s[^a-z0-9]*codex";
    static final String primaryNumberPattern = "(?:[^a-z0-9]*([0-9]{1,"+numLimit+"}))?";
    static final String secondaryNumberPattern = "(?:[^0-9]+([0-9]{1,"+numLimit+"}))?";
    static final String other = "(.*)?$";
    static final Pattern msCodexCallNoPattern = Pattern.compile(msCodexPattern + primaryNumberPattern + secondaryNumberPattern + other);
    static final int[] indices = new int[] {2, 4, 5};
    static final Padder zeroPads;

    static {
        zeroPads = new Padder(new StringBuilder(numLimit), numLimit, '0', false);
    }
    
    public String normalize(CharSequence input) {
        Matcher m = msCodexCallNoPattern.matcher(input);
        if (!m.matches()) {
            return null;
        } else {
            int[] groupIndices = new int[4];
            int maxIndex = 0;
            StringBuilder sb = new StringBuilder(input.length());
            for (int i = 1; i <= 3; i++) {
                String s = m.group(i);
                switch (i) {
                    case 1:
                    case 2:
                        if (zeroPads.appendPadded(sb, s, groupIndices, i)) maxIndex = i;
                        break;
                    case 3:
                        if (Padder.appendDirect(sb, s, groupIndices, i)) maxIndex = i;
                }
            }
            if (maxIndex < 2) {
                sb.setLength(groupIndices[maxIndex]);
            }
            return sb.toString();
        }
    }
    
}
