package edu.upenn.library.solrplugins.callno;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.util.TokenFilterFactory;

/**
 *
 * @author michael
 */
public class TypePrefixFilterFactory extends TokenFilterFactory {

    private static final String DELIM_ARGNAME = "delimiter";
    private static final String INCLUDE_ARGNAME = "includeTypes";
    private static final String EXCLUDE_ARGNAME = "excludeTypes";
    private static final String OUTPUTTYPE_ARGNAME = "outputType";
    private final char delim;
    private final Set<String> includeTypes;
    private final Set<String> excludeTypes;
    private final String outputType;
    
    public TypePrefixFilterFactory(Map<String, String> args) {
        super(args);
        String delimString = args.get(DELIM_ARGNAME);
        delim = delimString == null || delimString.length() == 0 ? (char) -1 : delimString.charAt(0);
        includeTypes = getTypeSet(args, INCLUDE_ARGNAME);
        excludeTypes = getTypeSet(args, EXCLUDE_ARGNAME);
        outputType = args.get(OUTPUTTYPE_ARGNAME);
    }
    
    private static Set<String> getTypeSet(Map<String, String> args, String argname) {
        String raw = args.get(argname);
        if (raw == null || raw.length() <= 0) {
            return null;
        } else {
            String[] types = raw.trim().split("\\s*,\\s*");
            if (types.length == 1) {
                return Collections.singleton(types[0]); 
            } else {
                return new HashSet<String>(Arrays.asList(types));
            }
        }
    }
    
    @Override
    public TokenStream create(TokenStream input) {
        return new TypePrefixFilter(input, delim, includeTypes, excludeTypes, outputType);
    }

}
