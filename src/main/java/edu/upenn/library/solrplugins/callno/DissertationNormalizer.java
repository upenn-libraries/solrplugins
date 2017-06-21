package edu.upenn.library.solrplugins.callno;

import java.util.Calendar;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 *
 * @author michael
 */
public class DissertationNormalizer implements Normalizer {

    static final int alphaLimit = 3;
    static final int numLimit = 4;
    static final String cutterPattern = "(?:[^a-z0-9]+([a-z]{1,"+alphaLimit+"})([0-9]{0,"+numLimit+"}))?";
    static final String dissPattern = "\\s*diss[^a-z0-9]*(poa|popm|posw?|pm)[^0-9a-z]*([0-9]{4})(?:\\.([0-9]+))?";
    static final String zeroPattern = "((?:[a-z]0[0-9]{2})|(?:[a-z]{2}0[0-9]))[^0-9]([0-9]{4})"+cutterPattern;
    static final String other = "(\\s.*)?$";
    static final Pattern diss = Pattern.compile(dissPattern + other);
    static final Pattern zero = Pattern.compile(zeroPattern + other);
    static final Padder zeroPads;
    static final Padder spacePads;

    static {
        StringBuilder sb = new StringBuilder(Math.max(numLimit, alphaLimit + 1));
        zeroPads = new Padder(sb, numLimit, '0', false);
        spacePads = new Padder(sb, alphaLimit + 1, ' ', true);
    }
    
    private static final String MAX_YEAR = Integer.toString(Calendar.getInstance().get(Calendar.YEAR) + 2);
    
    private String normalizeZero(CharSequence input) {
        Matcher m = zero.matcher(input);
        if (!m.matches()) {
            return null;
        } else {
            StringBuilder sb = new StringBuilder(input.length()).append('z');
            int[] groupIndexes = new int[6];
            int maxIndex = 0;
            for (int i = 1; i <= 5; i++) {
                String s = m.group(i);
                switch (i) {
                    case 1:
                        Padder.appendDirect(sb, s, groupIndexes, i);
                        maxIndex = i;
                        break;
                    case 2:
                        if (s.compareTo(MAX_YEAR) > 0) {
                            return null;
                        } else {
                            Padder.appendDirect(sb, s, groupIndexes, i);
                            maxIndex = i;
                        }
                        break;
                    case 3:
                        if (spacePads.appendPadded(sb, s, groupIndexes, i)) maxIndex = i;
                        break;
                    case 4:
                        if (zeroPads.appendPadded(sb, s, groupIndexes, i)) maxIndex = i;
                        break;
                    case 5:
                        if (Padder.appendDirect(sb, s, groupIndexes, i)) maxIndex = i;
                        break;
                }
            }
            if (maxIndex < 4) {
                sb.setLength(groupIndexes[maxIndex]);
            }
            return sb.toString();
        }
    }
    
    private String normalizeDiss(CharSequence input) {
        Matcher m = diss.matcher(input);
        if (!m.matches()) {
            return null;
        } else {
            StringBuilder sb = new StringBuilder(input.length()).append('d');
            int[] groupIndexes = new int[5];
            int maxIndex = 0;
            for (int i = 1; i <= 4; i++) {
                String s = m.group(i);
                switch (i) {
                    case 1:
                        if (spacePads.appendPadded(sb, s, groupIndexes, i)) maxIndex = i;
                        break;
                    case 2:
                        if (s.compareTo(MAX_YEAR) > 0) {
                            return null;
                        } else {
                            Padder.appendDirect(sb, s, groupIndexes, i);
                            maxIndex = i;
                        }
                        break;
                    case 3:
                        if (zeroPads.appendPadded(sb, s, groupIndexes, i)) maxIndex = i;
                        break;
                    case 4:
                        if (Padder.appendDirect(sb, s, groupIndexes, i)) maxIndex = i;
                        break;
                }
            }
            if (maxIndex < 3) {
                sb.setLength(groupIndexes[maxIndex]);
            }
            return sb.toString();
        }
    }
    
    public String normalize(CharSequence input) {
        String ret;
        if ((ret = normalizeZero(input)) != null) {
            return ret;
        } else if ((ret = normalizeDiss(input)) != null) {
            return ret;
        } else {
            return null;
        }
    }
    
}
