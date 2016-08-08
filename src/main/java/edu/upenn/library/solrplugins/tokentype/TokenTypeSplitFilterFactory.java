package edu.upenn.library.solrplugins.tokentype;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.util.ResourceLoader;
import org.apache.lucene.analysis.util.ResourceLoaderAware;
import org.apache.lucene.analysis.util.TokenFilterFactory;

/**
 *
 * @author michael
 */
public class TokenTypeSplitFilterFactory extends TokenFilterFactory implements ResourceLoaderAware {

  private static final String OUTPUT_FILTER_FACTORY_ARGNAME = "_class";
  private static final String INCLUDE_INPUT_TYPES_ARGNAME = "includeTypes";
  private static final String EXCLUDE_INPUT_TYPES_ARGNAME = "excludeTypes";
  private static final String INPUT_TYPE_RENAME_ARGNAME = "inputTypeRename";
  private static final String OUTPUT_TYPE_ARGNAME = "outputType";
  private static final char SUBARG_PREFIX = '_';

  private final String outputFilterFactoryName;
  private TokenFilterFactory outputFilterFactory;
  private final Set<String> includeInput;
  private final Set<String> excludeInput;
  private final String outputType;
  private final String inputTypeRename;
  private final Map<String, String> subargs;

  public TokenTypeSplitFilterFactory(Map<String, String> args) {
    super(args);
    outputFilterFactoryName = args.containsKey(OUTPUT_FILTER_FACTORY_ARGNAME) ? args.get(OUTPUT_FILTER_FACTORY_ARGNAME) : null;
    inputTypeRename = args.get(INPUT_TYPE_RENAME_ARGNAME);
    outputType = args.get(OUTPUT_TYPE_ARGNAME);
    includeInput = parseTypeNames(args.get(INCLUDE_INPUT_TYPES_ARGNAME));
    excludeInput = parseTypeNames(args.get(EXCLUDE_INPUT_TYPES_ARGNAME));
    HashMap<String, String> sub = new HashMap<String, String>();
    for (Entry<String, String> e : args.entrySet()) {
      if (e.getKey().charAt(0) == SUBARG_PREFIX) {
        sub.put(e.getKey().substring(1), e.getValue());
      }
    }
    switch (sub.size()) {
      case 0:
        subargs = Collections.EMPTY_MAP;
        break;
      case 1:
        Entry<String, String> e = sub.entrySet().iterator().next();
        subargs = Collections.singletonMap(e.getKey(), e.getValue());
        break;
      default:
        subargs = sub;
    }
  }

  private static Set<String> parseTypeNames(String typeNames) {
    if (typeNames == null) {
      return null;
    } else {
      String[] nameArray = typeNames.split("\\s*,\\s*");
      switch (nameArray.length) {
        case 0:
          return null;
        case 1:
          return Collections.singleton(nameArray[0]);
        default:
          return new HashSet<String>(Arrays.asList(nameArray));
      }
    }
  }

  @Override
  public TokenStream create(TokenStream input) {
    TokenTypeSplitFilter local = new TokenTypeSplitFilter(input, includeInput, excludeInput, outputType, inputTypeRename);
    if (outputFilterFactory == null) {
      return local;
    } else {
      TokenStream output = outputFilterFactory.create(local);
      local.setDelegate(output);
      return local;
    }
  }

  public void inform(ResourceLoader loader) throws IOException {
    if (outputFilterFactoryName == null) {
      outputFilterFactory = null;
    } else {
      Class<? extends TokenFilterFactory> offClass = loader.findClass(outputFilterFactoryName, TokenFilterFactory.class);
      try {
        Constructor<? extends TokenFilterFactory> constructor = offClass.getConstructor(Map.class);
        outputFilterFactory = constructor.newInstance(subargs);
        if (outputFilterFactory instanceof ResourceLoaderAware) {
          ((ResourceLoaderAware)outputFilterFactory).inform(loader);
        }
      } catch (NoSuchMethodException ex) {
        throw new RuntimeException(ex);
      } catch (SecurityException ex) {
        throw new RuntimeException(ex);
      } catch (InstantiationException ex) {
        throw new RuntimeException(ex);
      } catch (IllegalAccessException ex) {
        throw new RuntimeException(ex);
      } catch (IllegalArgumentException ex) {
        throw new RuntimeException(ex);
      } catch (InvocationTargetException ex) {
        throw new RuntimeException(ex);
      }
    }
  }

}
