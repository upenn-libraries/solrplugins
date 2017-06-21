package edu.upenn.library.solrplugins.callno;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 *
 * @author michael
 */
public class DVDNormalizer implements Normalizer {

    static final String other = "(?:\\s(.+)?)?$";
    static final Pattern prefix = Pattern.compile("^dvd(?:/pal)?\\s*");
    static final Pattern numeric = Pattern.compile("([0-9]{3})\\s+([0-9]{3})"+other);

    private static final LCNormalizer lcNorm = new LCNormalizer();
    
    private String normalizeNumeric(CharSequence input) {
        Matcher m = numeric.matcher(input);
        if (!m.matches()) {
            return null;
        } else {
            StringBuilder sb = new StringBuilder(input.length());
            for (int i = 1; i <= 3; i++) {
                String s = m.group(i);
                switch (i) {
                    case 1:
                    case 2:
                        sb.append(s);
                        break;
                    case 3:
                        if (s != null) {
                            sb.append(s);
                        }
                        break;
                }
            }
            return sb.toString();

        }
    }
        
    @Override
    public String normalize(CharSequence input) {
        Matcher m = prefix.matcher(input);
        if (!m.find()) {
            return null;
        } else {
            CharSequence mainPart = input.subSequence(m.end(), input.length());
            String ret;
            if ((ret = normalizeNumeric(mainPart)) != null) {
                return ret;
            } else if ((ret = lcNorm.normalize(mainPart)) != null) {
                return ret;
            } else {
                return null;
            }
        }
    }

}
