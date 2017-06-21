package edu.upenn.library.solrplugins.callno;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 *
 * @author michael
 */
public class CDNormalizer implements Normalizer {

    static final int alphaLimit = 10;
    static final int alphaCutterLimit = 3;
    static final int numLimit = 10;
    static final int numCutterLimit = 5;
    static final String leading = "^cd ?([0-9]{5})";
    static final String cutter = "(?:[^a-z0-9]([a-z]{0,"+alphaCutterLimit+"})[^a-z0-9\\s]?([0-9]{1,"+numCutterLimit+"}))?";
    static final String other = "(?:\\s(.+)?)?";
    static final Pattern trailing = Pattern.compile("^(cd oversize )?([^0-9]*[a-z][^0-9]*)([0-9]{1,"+numLimit+"})"+cutter+cutter+other+"cd\\s*$");
    static final Pattern p = Pattern.compile(leading + cutter + cutter + other + "$");
    static final int indices = 5;
    static final Padder zeroPads;
    static final Padder spacePads;
    static final Padder zeroPadsCutter;
    static final Padder spacePadsCutter;

    static {
        StringBuilder sb = new StringBuilder();
        zeroPads = new Padder(sb, numLimit, '0', false);
        spacePads = new Padder(sb, alphaLimit, ' ', true);
        zeroPadsCutter = new Padder(sb, numCutterLimit, '0', false);
        spacePadsCutter = new Padder(sb, alphaCutterLimit, ' ', true);
    }
    
    private static final Pattern nonAlpha = Pattern.compile("[^a-z]+");
    
    private String normalizeLeading(CharSequence input) {
        Matcher m = p.matcher(input);
        if (!m.matches()) {
            return null;
        } else {
            int[] groupIndexes = new int[7];
            int maxIndex = 0;
            StringBuilder sb = new StringBuilder(input.length());
            for (int i = 1; i <= 6; i++) {
                String s = m.group(i);
                switch (i) {
                    case 1:
                        Padder.appendStrict(sb, s, groupIndexes, i);
                        maxIndex = i;
                        break;
                    case 2:
                    case 4:
                        if (spacePadsCutter.appendPadded(sb, s, groupIndexes, i)) maxIndex = i;
                        break;
                    case 3:
                    case 5:
                        if (zeroPadsCutter.appendPadded(sb, s, groupIndexes, i)) maxIndex = i;
                        break;
                    case 6:
                        if (Padder.appendDirect(sb, s, groupIndexes, i)) maxIndex = i;
                        break;
                }
            }
            if (maxIndex < 5) {
                sb.setLength(groupIndexes[maxIndex]);
            }
            return sb.toString();

        }
    }
    
    private String normalizeTrailing(CharSequence input) {
        Matcher m = trailing.matcher(input);
        if (!m.matches()) {
            return null;
        } else {
            int[] groupIndexes = new int[9];
            int maxIndex = 0;
            StringBuilder sb = new StringBuilder(input.length());
            for (int i = 1; i <= 8; i++) {
                String s = m.group(i);
                switch (i) {
                    case 1:
                        if (s != null) {
                            Padder.appendStrict(sb, 'V', groupIndexes, i);
                            maxIndex = i;
                        }
                        break;
                    case 2:
                        if (spacePads.appendVariable(sb, s, groupIndexes, i, nonAlpha, " ")) maxIndex = i;
                        break;
                    case 3:
                        if (zeroPads.appendPadded(sb, s, groupIndexes, i)) maxIndex = i;
                        break;
                    case 4:
                    case 6:
                        if (spacePadsCutter.appendPadded(sb, s, groupIndexes, i)) maxIndex = i;
                        break;
                    case 5:
                    case 7:
                        if (zeroPadsCutter.appendPadded(sb, s, groupIndexes, i)) maxIndex = i;
                        break;
                    case 8:
                        if (Padder.appendDirect(sb, s, groupIndexes, i)) maxIndex = i;
                        break;
                }
            }
            if (maxIndex < 7) {
                sb.setLength(groupIndexes[maxIndex]);
            }
            return sb.toString();
        }
    }
    
    @Override
    public String normalize(CharSequence input) {
        String ret;
        if ((ret = normalizeLeading(input)) != null) {
            return ret;
        } else if ((ret = normalizeTrailing(input)) != null) {
            return ret;
        } else {
            return null;
        }
    }
    
}
