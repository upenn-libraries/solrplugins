/*
 * Copyright 2017 The Trustees of the University of Pennsylvania
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
package edu.upenn.library.solrplugins;

import edu.upenn.library.solrplugins.tokentype.TokenTypeJoinFilter;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor.CallerRunsPolicy;
import java.util.concurrent.TimeUnit;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.PayloadAttribute;
import org.apache.lucene.analysis.util.TokenFilterFactory;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.SolrInputField;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.schema.JsonPreAnalyzedParser;
import org.apache.solr.schema.PreAnalyzedField;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.update.AddUpdateCommand;
import org.apache.solr.update.processor.UpdateRequestProcessor;
import org.apache.solr.update.processor.UpdateRequestProcessorFactory;

/**
 *
 *
 * <code>
 *   <updateProcessor class="edu.upenn.library.solrplugins.XrefUpdateRequestProcessorFactory" name="xref-copyfield">
 *     [<str name="bufferDocs">[true|false]</str>]
 *     <lst name="[inputFieldName]">
 *       <str name="outputField">[outputFieldName]</str>
 *       <lst name="copyFieldFilters">
 *         <lst name="[copyfieldDestFieldName]">
 *           <str name="class">[TokenFilterFactory class name]</str>
 *           [<str name="[initArgName]">[initArgValue]</str>]*
 *         </lst>
 *       </lst>
 *     </lst>
 *   </updateProcessor>
 * </code>
 * @author magibney
 */
public class XrefUpdateRequestProcessorFactory extends UpdateRequestProcessorFactory {

  private static final boolean DEFAULT_BUFFER_DOCS = true;
  private static final JsonPreAnalyzedParser parser = new JsonPreAnalyzedParser();
  private static final String OUTPUT_FIELD_ARGNAME = "outputField";
  private static final String COPY_FIELD_DEST_ARGNAME = "copyFieldDest";
  private static final String COPY_FIELD_FILTERS_ARGNAME = "copyFieldFilters";
  private static final String BUFFER_DOCS_ARGNAME = "bufferDocs";

  private boolean bufferDocs = DEFAULT_BUFFER_DOCS;
  private final Map<String, Entry<String, List<CopyFieldFilterFactory>>> config = new HashMap<>();
  
  @Override
  public UpdateRequestProcessor getInstance(SolrQueryRequest req, SolrQueryResponse rsp, UpdateRequestProcessor next) {
    IndexSchema schema = req.getSchema();
    return new XrefUpdateRequestProcessor(next, config, schema, bufferDocs);
  }

  @Override
  public void init(NamedList args) {
    Iterator<Entry<String, Object>> iter = args.iterator();
    while (iter.hasNext()) {
      Entry<String, Object> next = iter.next();
      Object val = next.getValue();
      if (val instanceof String) {
        // treat as String arg
        if (BUFFER_DOCS_ARGNAME.equals(next.getKey())) {
          bufferDocs = Boolean.parseBoolean((String) val);
        }
      } else if (val instanceof NamedList) {
        // treat as copyField entry
        NamedList entryConfig = (NamedList) val;
        String inputField = next.getKey();
        String outputField = (String) entryConfig.get(OUTPUT_FIELD_ARGNAME);
        NamedList<Object> copyFieldConfig = (NamedList) entryConfig.get(COPY_FIELD_FILTERS_ARGNAME);
        List<CopyFieldFilterFactory> filterFactories;
        if (copyFieldConfig == null || copyFieldConfig.size() < 1) {
          filterFactories = null;
        } else {
          filterFactories = new ArrayList<>(copyFieldConfig.size());
          for (Entry<String, Object> nextCopyField : copyFieldConfig) {
            String destFieldName = nextCopyField.getKey();
            NamedList<String> filterFactoryConfig = (NamedList)nextCopyField.getValue();
            String factoryClassName = (String)filterFactoryConfig.remove("class");
            Map<String, String> factoryInitArgs;
            if (filterFactoryConfig.size() < 1) {
              factoryInitArgs = Collections.singletonMap(COPY_FIELD_DEST_ARGNAME, destFieldName);
            } else {
              factoryInitArgs = new HashMap<>(filterFactoryConfig.size() + 1);
              for (Entry<String, String> extraFactoryInitArg : filterFactoryConfig) {
                factoryInitArgs.put(extraFactoryInitArg.getKey(), extraFactoryInitArg.getValue());
              }
              factoryInitArgs.put(COPY_FIELD_DEST_ARGNAME, destFieldName);
            }
            try {
              Constructor<?> constructor = Class.forName(factoryClassName).getConstructor(Map.class);
              CopyFieldFilterFactory filterFactory = (CopyFieldFilterFactory)constructor.newInstance(factoryInitArgs);
              filterFactories.add(filterFactory);
            } catch (ClassCastException | ClassNotFoundException | InstantiationException | IllegalAccessException |
                NoSuchMethodException | SecurityException | IllegalArgumentException | InvocationTargetException ex) {
              System.err.println("ERROR: cannot load copyFieldFilterFactory: " + factoryClassName);
              ex.printStackTrace(System.err);
            }
          }
        }
        config.put(inputField, new SimpleImmutableEntry<>(outputField, filterFactories));
      }
    }
    super.init(args);
  }

//  public static void main(String[] args) throws Exception {
//    SchemaField sf = new SchemaField("inputFieldName", new TextField() {
//      @Override
//      public Analyzer getIndexAnalyzer() {
//        return new Analyzer() {
//
//          @Override
//          protected Analyzer.TokenStreamComponents createComponents(String fieldName) {
//            KeywordTokenizer kt = new KeywordTokenizer();
//            TokenTypeSplitFilter ttsf = new TokenTypeSplitFilter(kt, null, null, "normalized", "filing");
//            Map<String, Integer> inputTypes = new HashMap<>(4);
//            inputTypes.put("normalized", 0);
//            inputTypes.put("filing", 1);
//            TokenTypeJoinFilter ttjf = new TokenTypeJoinFilter(ttsf, inputTypes, "indexed", "filing", "!", false, true, new int[] {1});
//            return new TokenStreamComponents(kt, ttjf);
//          }
//        };
//      }
//
//    });
//    SchemaField outputFieldSF = new SchemaField("outputFieldName", new TextField());
//    XrefUpdateRequestProcessor blah = new XrefUpdateRequestProcessor(null, sf, outputFieldSF, 
//        Collections.singletonList(new NullPayloadCopyFieldFilterFactory(Collections.singletonMap(COPY_FIELD_DEST_ARGNAME, "copyFieldName"))), false);
//    AddUpdateCommand cmd = new AddUpdateCommand(null);
//    SolrInputField sif = new SolrInputField("inputFieldName");
//    for (int i = 0; i < 100; i++) {
//      sif.addValue("value" + i, 0);
//    }
//    Map<String, SolrInputField> docMap = new HashMap<>(1);
//    docMap.put("inputFieldName", sif);
//    cmd.solrDoc = new SolrInputDocument(docMap);
//    long start = System.currentTimeMillis();
//    blah.processAdd(cmd);
//    blah.close();
//    System.err.println(cmd.solrDoc);
//    System.err.println("duration: "+(System.currentTimeMillis() - start));
//  }
  
  private static class XrefUpdateRequestProcessor extends UpdateRequestProcessor {

    private Map<String, FieldStruct> config;
    private final ExecutorService executor;
    private ExecutorCompletionService<ParallelAnalysisResult> exec;
    private int bufferedDocCount = 0;
    private final boolean bufferDocs;

    @Override
    protected void doClose() {
      executor.shutdown();
      try {
        executor.awaitTermination(10, TimeUnit.SECONDS);
      } catch (InterruptedException ex) {
        throw new RuntimeException(ex);
      } finally {
        super.doClose();
      }
    }

    private static class FieldStruct {

      private final String outputField;
      private final FieldType outputFieldType;
      private final List<CopyFieldFilterFactory> filters;
      private final Analyzer analyzer;
      private Collection<Object> vals;

      public FieldStruct(String outputField, FieldType outputFieldType, List<CopyFieldFilterFactory> filters, Analyzer analyzer) {
        this.outputField = outputField;
        this.outputFieldType = outputFieldType;
        this.filters = filters;
        this.analyzer = analyzer;
      }
    }

    private final CallerRunsPolicy callerRuns = new CallerRunsPolicy();
    
    public XrefUpdateRequestProcessor(UpdateRequestProcessor next, Map<String, Entry<String, List<CopyFieldFilterFactory>>> config, 
        IndexSchema schema, boolean bufferDocs) {
      super(next);
      this.config = new HashMap<>(config.size());
      for (Entry<String, Entry<String, List<CopyFieldFilterFactory>>> e : config.entrySet()) {
        String inputFieldName = e.getKey();
        SchemaField inputField = schema.getField(inputFieldName);
        if (inputField == null) {
          continue;
        }
        Entry<String, List<CopyFieldFilterFactory>> destEntry = e.getValue();
        String outputFieldName = destEntry.getKey();
        SchemaField outputField = schema.getField(outputFieldName);
        if (outputField == null) {
          continue;
        }
        Analyzer analyzer = inputField.getType().getIndexAnalyzer();
        FieldType outputFieldType = PreAnalyzedField.createFieldType(outputField);
        FieldStruct fieldStruct = new FieldStruct(outputFieldName, outputFieldType, destEntry.getValue(), analyzer);
        this.config.put(inputFieldName, fieldStruct);
      }
      this.executor = new ThreadPoolExecutor(0, 20, 10, TimeUnit.SECONDS, new ArrayBlockingQueue<>(20), callerRuns);
      this.bufferDocs = bufferDocs;
    }

    @Override
    public void processAdd(AddUpdateCommand cmd) throws IOException {
      SolrInputDocument doc = cmd.getSolrInputDocument();
      int termCount = 0;
      int maxTermsPerField = 0;
      for (Entry<String, FieldStruct> e : config.entrySet()) {
        String inputFieldName = e.getKey();
        SolrInputField removeField = doc.removeField(inputFieldName);
        FieldStruct fieldStruct = e.getValue();
        Collection<Object> fieldInputVals;
        if (removeField == null || (fieldInputVals = removeField.getValues()) == null || fieldInputVals.isEmpty()) {
          fieldStruct.vals = null;
        } else {
          fieldStruct.vals = fieldInputVals;
          int termsPerField = fieldInputVals.size();
          termCount += termsPerField;
          if (termsPerField > maxTermsPerField) {
            maxTermsPerField = termsPerField;
          }
        }
      }
      if (termCount < 1) {
        super.processAdd(cmd);
        return;
      }
      final boolean parallel;
      final int[] docCount;
      final Map<String, Object>[] docFields;
      Map<String, Object>[] replacements = null;
      if (exec != null) {
        parallel = true;
        docCount = new int[]{termCount};
        docFields = new Map[termCount];
        bufferedDocCount++; // the incoming doc
      } else if (termCount > 1 || !cmd.isLastDocInBatch) {
        parallel = true;
        docCount = new int[]{termCount};
        docFields = new Map[termCount];
        bufferedDocCount = 1; // the incoming doc
        exec = new ExecutorCompletionService<>(executor);
      } else {
        parallel = false;
        docCount = null;
        docFields = null;
        replacements = new Map[termCount];
      }
      int i = 0;
      for (Entry<String, FieldStruct> e : config.entrySet()) {
        FieldStruct fieldStruct = e.getValue();
        if (fieldStruct.vals != null) {
          String inputFieldName = e.getKey();
          Analyzer analyzer = fieldStruct.analyzer;
          String outputField = fieldStruct.outputField;
          FieldType outputFieldType = fieldStruct.outputFieldType;
          List<CopyFieldFilterFactory> filters = fieldStruct.filters;
          for (Object val : fieldStruct.vals) {
            if (parallel) {
              exec.submit(new ParallelAnalyzer(i, inputFieldName, (String)val, analyzer, outputField, outputFieldType,
                  filters, cmd, docCount, docFields));
            } else {
              replacements[i] = preAnalyze(inputFieldName, (String)val, analyzer, outputField, outputFieldType, filters);
            }
            i++;
          }
        }
      }
      List<Entry<AddUpdateCommand, Map<String, Object>[]>> completed;
      if (!parallel) {
        completed = Collections.singletonList(new SimpleImmutableEntry<>(cmd, replacements));
      } else {
        completed = new LinkedList<>();
        Future<ParallelAnalysisResult> future;
        try {
          while ((future = (bufferDocs && !cmd.isLastDocInBatch) ? exec.poll() : exec.take()) != null) {
            ParallelAnalysisResult res;
            try {
              res = future.get();
            } catch (ExecutionException ex) {
              throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, ex);
            }
            res.docFields[res.index] = res.fieldReplacements;
            if (--res.docCount[0] == 0) {
              completed.add(new SimpleImmutableEntry<>(res.cmd, res.docFields));
              if (--bufferedDocCount <= 0) {
                break;
              }
            }
          }
        } catch (InterruptedException ex) {
          throw new RuntimeException(ex);
        }
      }
      for (Entry<AddUpdateCommand, Map<String, Object>[]> docEntry : completed) {
        cmd = docEntry.getKey();
        doc = cmd.solrDoc;
        replacements = docEntry.getValue();
        for (String inputFieldName : config.keySet()) {
          doc.removeField(inputFieldName);
        }
        Map<String, Collection<Object>> buildValues = new HashMap<>();
        for (Map<String, Object> replacement : replacements) {
          for (Entry<String, Object> e : replacement.entrySet()) {
            String fieldName = e.getKey();
            Collection<Object> vals = buildValues.get(fieldName);
            if (vals == null) {
              vals = new ArrayList<>(maxTermsPerField);
              buildValues.put(fieldName, vals);
            }
            Object val = e.getValue();
            if (val instanceof Collection) {
              vals.addAll((Collection<Object>) val);
            } else {
              vals.add(val);
            }
          }
        }
        for (Entry<String, Collection<Object>> e : buildValues.entrySet()) {
          Collection<Object> val = e.getValue();
          if (val != null && !val.isEmpty()) {
            doc.setField(e.getKey(), e.getValue());
          }
        }
        super.processAdd(cmd);
      }
    }

  }
  
  private static class NullPayloadCopyFieldFilter extends CopyFieldTokenFilter {

    private final TokenTypeJoinFilter.DisplayAttribute displayAtt = getAttribute(TokenTypeJoinFilter.DisplayAttribute.class);
    private final PayloadAttribute payloadAtt = getAttribute(PayloadAttribute.class);
    private final String copyFieldDest;
    private final ArrayList<String> vals = new ArrayList<>();
    private boolean done = false;
    
    public NullPayloadCopyFieldFilter(TokenStream input, String copyFieldDest) {
      super(input);
      this.copyFieldDest = copyFieldDest;
    }

    @Override
    public boolean incrementToken() throws IOException {
      boolean ret = input.incrementToken();
      if (!ret) {
        return false;
      }
      if (payloadAtt.getPayload() == null) {
        vals.add(displayAtt.toString());
      }
      displayAtt.setEmpty();
      return ret;
    }

    @Override
    public void reset() throws IOException {
      done = false;
      displayAtt.setEmpty();
      vals.clear();
      super.reset();
    }

    @Override
    public void end() throws IOException {
      done = true;
      super.end(); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public Entry<String, Collection<Object>> getCopyFields() {
      if (!done) {
        throw new IllegalStateException();
      } else {
        return vals.isEmpty() ? null : new SimpleImmutableEntry<>(copyFieldDest, new ArrayList<>(vals));
      }
    }
    
  }
  
  public static class NullPayloadCopyFieldFilterFactory extends CopyFieldFilterFactory {

    public NullPayloadCopyFieldFilterFactory(Map<String, String> args) {
      super(args);
    }

    @Override
    public CopyFieldTokenFilter create(TokenStream ts) {
      return new NullPayloadCopyFieldFilter(ts, copyFieldDest);
    }
    
  }
  
  public static abstract class CopyFieldTokenFilter extends TokenFilter {

    public CopyFieldTokenFilter(TokenStream input) {
      super(input);
    }
    
    public abstract Entry<String, Collection<Object>> getCopyFields();
  } 
  
  public static abstract class CopyFieldFilterFactory extends TokenFilterFactory {

    protected final String copyFieldDest;
    
    public CopyFieldFilterFactory(Map<String, String> args) {
      super(args);
      this.copyFieldDest = args.get(COPY_FIELD_DEST_ARGNAME);
    }
    
    @Override
    public abstract CopyFieldTokenFilter create(TokenStream stream);
    
  }
  
  private static Map<String, Object> preAnalyze(String inputFieldName, String input, Analyzer analyzer, String outputFieldName, 
      FieldType outputFieldType, List<CopyFieldFilterFactory> filters) throws IOException {
    TokenStream ts = analyzer.tokenStream(inputFieldName, input);
    List<CopyFieldTokenFilter> copyFieldFilters;
    if (filters == null || filters.isEmpty()) {
      copyFieldFilters = null;
    } else {
      copyFieldFilters = new ArrayList<>(filters.size());
      for (CopyFieldFilterFactory cfff : filters) {
        CopyFieldTokenFilter cff = cfff.create(ts);
        copyFieldFilters.add(cff);
        ts = cff;
      }
    }
    ts.reset();
    String tsJson = parser.toFormattedString(new Field(outputFieldName, ts, outputFieldType));
    ts.end();
    ts.close();
    if (copyFieldFilters == null || copyFieldFilters.isEmpty()) {
      return Collections.singletonMap(outputFieldName, tsJson);
    } else {
      Map<String, Object> ret = new HashMap<>(copyFieldFilters.size() + 1);
      ret.put(outputFieldName, tsJson);
      for (CopyFieldTokenFilter cff : copyFieldFilters) {
        Entry<String, Collection<Object>> entry = cff.getCopyFields();
        if (entry != null) {
          Collection<Object> val = entry.getValue();
          if (val != null && !val.isEmpty()) {
            ret.put(entry.getKey(), val);
          }
        }
      }
      return ret;
    }
  }

  private static class ParallelAnalysisResult {
    private final AddUpdateCommand cmd;
    private final Map<String, Object> fieldReplacements;
    private final int index;
    private final int[] docCount;
    private final Map<String, Object>[] docFields;

    public ParallelAnalysisResult(AddUpdateCommand cmd, Map<String, Object> fieldReplacements, int[] docCount,
        Map<String, Object>[] docFields, int index) {
      this.cmd = cmd;
      this.fieldReplacements = fieldReplacements;
      this.index = index;
      this.docCount = docCount;
      this.docFields = docFields;
    }
  }
  
  private static class ParallelAnalyzer implements Callable<ParallelAnalysisResult> {

    private final int index;
    private final String fieldName;
    private final String input;
    private final Analyzer analyzer;
    private final String outputFieldName;
    private final FieldType outputFieldType;
    private final List<CopyFieldFilterFactory> filters;
    private final AddUpdateCommand cmd;
    private final int[] docCount;
    private final Map<String, Object>[] docFields;
    
    public ParallelAnalyzer(int index, String fieldName, String input, Analyzer analyzer, String outputFieldName, 
        FieldType outputFieldType, List<CopyFieldFilterFactory> filters, AddUpdateCommand cmd, int[] docCount,
        Map<String, Object>[] docFields) {
      this.index = index;
      this.fieldName = fieldName;
      this.input = input;
      this.analyzer = analyzer;
      this.outputFieldName = outputFieldName;
      this.outputFieldType = outputFieldType;
      this.filters = filters;
      this.cmd = cmd;
      this.docCount = docCount;
      this.docFields = docFields;
    }
    
    @Override
    public ParallelAnalysisResult call() {
      try {
        Map<String, Object> analysisResult = preAnalyze(fieldName, input, analyzer, outputFieldName, outputFieldType, filters);
        return new ParallelAnalysisResult(cmd, analysisResult, docCount, docFields, index);
      } catch (IOException ex) {
        ex.printStackTrace(System.err);
        throw new RuntimeException(ex);
      }
    }

  }
}
