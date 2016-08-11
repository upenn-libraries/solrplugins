/*
 * Copyright 2016 The Trustees of the University of Pennsylvania
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0

 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
public class TokenTypeProcessFilterFactory extends TokenFilterFactory implements ResourceLoaderAware {

  private static final String DELEGATE_FILTER_FACTORY_ARGNAME = "_class";
  private static final String INCLUDE_INPUT_TYPES_ARGNAME = "includeTypes";
  private static final String EXCLUDE_INPUT_TYPES_ARGNAME = "excludeTypes";
  private static final String INPUT_TYPE_RENAME_ARGNAME = "inputTypeRename";
  private static final char SUBARG_PREFIX = '_';

  private final String delegateFilterFactoryName;
  private TokenFilterFactory delegateFilterFactory;
  private final Set<String> includeInput;
  private final Set<String> excludeInput;
  private final String inputTypeRename;
  private final Map<String, String> subargs;

  public TokenTypeProcessFilterFactory(Map<String, String> args) {
    super(args);
    if (args.containsKey(DELEGATE_FILTER_FACTORY_ARGNAME)) {
      delegateFilterFactoryName = args.get(DELEGATE_FILTER_FACTORY_ARGNAME);
    } else {
      throw new IllegalArgumentException("must specify \"" + DELEGATE_FILTER_FACTORY_ARGNAME + "\" arg");
    }
    inputTypeRename = args.get(INPUT_TYPE_RENAME_ARGNAME);
    includeInput = parseTypeNames(args.get(INCLUDE_INPUT_TYPES_ARGNAME));
    excludeInput = parseTypeNames(args.get(EXCLUDE_INPUT_TYPES_ARGNAME));
    HashMap<String, String> sub = new HashMap<String, String>();
    for (Entry<String, String> e : args.entrySet()) {
      if (e.getKey().charAt(0) == SUBARG_PREFIX) {
        sub.put(e.getKey().substring(1), e.getValue());
      }
    }
    subargs = sub;
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
    TokenTypeProcessFilter local = new TokenTypeProcessFilter(input, includeInput, excludeInput, inputTypeRename);
    if (delegateFilterFactory == null) {
      return local;
    } else {
      TokenStream output = delegateFilterFactory.create(local);
      local.setDelegate(output);
      return local;
    }
  }

  @Override
  public void inform(ResourceLoader loader) throws IOException {
    if (delegateFilterFactoryName == null) {
      delegateFilterFactory = null;
    } else {
      Class<? extends TokenFilterFactory> offClass = loader.findClass(delegateFilterFactoryName, TokenFilterFactory.class);
      try {
        Constructor<? extends TokenFilterFactory> constructor = offClass.getConstructor(Map.class);
        delegateFilterFactory = constructor.newInstance(subargs);
        if (delegateFilterFactory instanceof ResourceLoaderAware) {
          ((ResourceLoaderAware)delegateFilterFactory).inform(loader);
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
