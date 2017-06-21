package edu.upenn.library.solrplugins.callno;

import java.io.IOException;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.util.ResourceLoader;
import org.apache.lucene.analysis.util.ResourceLoaderAware;
import org.apache.lucene.analysis.util.TokenFilterFactory;

/**
 *
 * @author michael
 */
public class CallNoFilterFactory extends TokenFilterFactory implements ResourceLoaderAware {

    private static final String NORMALIZERS_ARGNAME = "normalizers";
    private static final String DEFAULT_TYPE_ARGNAME = "defaultType";
    private static final String DEFAULT_PACKAGE_ARGNAME = "defaultPackage";
    private final String normalizersConfigString;
    private final String defaultType;
    private final String defaultPackagePrefix;
    private Entry<String, Normalizer>[] normalizers;
    
    public CallNoFilterFactory(Map<String, String> args) {
        super(args);
        normalizersConfigString = args.get(NORMALIZERS_ARGNAME).trim();
        defaultType = args.get(DEFAULT_TYPE_ARGNAME);
        String tmpPackage = args.get(DEFAULT_PACKAGE_ARGNAME);
        if (tmpPackage == null) {
            defaultPackagePrefix = getClass().getPackage().getName().concat(".");
        } else {
            defaultPackagePrefix = tmpPackage.concat(".");
        }
    }
    
    @Override
    public TokenStream create(TokenStream input) {
        return new CallNoFilter(input, normalizers, defaultType);
    }

    @Override
    public void inform(ResourceLoader loader) throws IOException {
        String[] entries = normalizersConfigString.split("\\s*,\\s");
        normalizers = new Entry[entries.length];
        for (int i = 0; i < entries.length; i++) {
            String e = entries[i];
            int delimIndex = e.indexOf(':');
            String type;
            String className;
            if (delimIndex > 0) {
                type = e.substring(0, delimIndex);
                className = e.substring(delimIndex + 1);
            } else {
                type = null;
                className = e;
            }
            if (className.indexOf('.') < 0) {
                className = defaultPackagePrefix.concat(className);
            }
            Normalizer norm = loader.newInstance(className, Normalizer.class);
            normalizers[i] = new SimpleImmutableEntry<>(type, norm);
        }
    }
    
}
