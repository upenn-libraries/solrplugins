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
import edu.upenn.library.solrplugins.tokentype.TokenTypeSplitFilter;
import java.io.IOException;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
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
import org.apache.lucene.analysis.core.KeywordTokenizer;
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
import org.apache.solr.schema.TextField;
import org.apache.solr.update.AddUpdateCommand;
import org.apache.solr.update.processor.UpdateRequestProcessor;
import org.apache.solr.update.processor.UpdateRequestProcessorFactory;

/**
 *
 * @author magibney
 */
public class XrefUpdateRequestProcessorFactory extends UpdateRequestProcessorFactory {

  private static final boolean DEFAULT_BUFFER_DOCS = true;
  private static final JsonPreAnalyzedParser parser = new JsonPreAnalyzedParser();
  private static final String INPUT_FIELD_ARGNAME = "inputField";
  private static final String OUTPUT_FIELD_ARGNAME = "outputField";
  private static final String COPY_FIELD_DEST_ARGNAME = "copyFieldDest";
  private static final String BUFFER_DOCS_ARGNAME = "bufferDocs";

  private String inputField;
  private String outputField;
  private String copyFieldDest;
  private boolean bufferDocs;
  
  @Override
  public UpdateRequestProcessor getInstance(SolrQueryRequest req, SolrQueryResponse rsp, UpdateRequestProcessor next) {
    IndexSchema schema = req.getSchema();
    return new XrefUpdateRequestProcessor(next, schema.getField(inputField), schema.getField(outputField), 
        Collections.singletonList(new CFTFImplFactory(Collections.EMPTY_MAP, copyFieldDest)), bufferDocs);
  }

  @Override
  public void init(NamedList args) {
    this.inputField = (String) args.get(INPUT_FIELD_ARGNAME);
    this.outputField = (String) args.get(OUTPUT_FIELD_ARGNAME);
    this.copyFieldDest = (String) args.get(COPY_FIELD_DEST_ARGNAME);
    Boolean bufferDocs = args.getBooleanArg(BUFFER_DOCS_ARGNAME);
    this.bufferDocs = bufferDocs != null ? bufferDocs : DEFAULT_BUFFER_DOCS;
    super.init(args);
  }

  public static void main(String[] args) throws Exception {
    SchemaField sf = new SchemaField("inputFieldName", new TextField() {
      @Override
      public Analyzer getIndexAnalyzer() {
        return new Analyzer() {

          @Override
          protected Analyzer.TokenStreamComponents createComponents(String fieldName) {
            KeywordTokenizer kt = new KeywordTokenizer();
            TokenTypeSplitFilter ttsf = new TokenTypeSplitFilter(kt, null, null, "normalized", "filing");
            Map<String, Integer> inputTypes = new HashMap<>(4);
            inputTypes.put("normalized", 0);
            inputTypes.put("filing", 1);
            TokenTypeJoinFilter ttjf = new TokenTypeJoinFilter(ttsf, inputTypes, "indexed", "filing", "!", false, true, new int[] {1});
            return new TokenStreamComponents(kt, ttjf);
          }
        };
      }

    });
    SchemaField outputFieldSF = new SchemaField("outputFieldName", new TextField());
    XrefUpdateRequestProcessor blah = new XrefUpdateRequestProcessor(null, sf, outputFieldSF, 
        Collections.singletonList(new CFTFImplFactory(Collections.EMPTY_MAP, "copyFieldName")), false);
    AddUpdateCommand cmd = new AddUpdateCommand(null);
    SolrInputField sif = new SolrInputField("inputFieldName");
    for (int i = 0; i < 100; i++) {
      sif.addValue("value" + i, 0);
    }
    Map<String, SolrInputField> docMap = new HashMap<>(1);
    docMap.put("inputFieldName", sif);
    cmd.solrDoc = new SolrInputDocument(docMap);
    long start = System.currentTimeMillis();
    blah.processAdd(cmd);
    blah.close();
    System.err.println(cmd.solrDoc);
    System.err.println("duration: "+(System.currentTimeMillis() - start));
  }
  
  private static class XrefUpdateRequestProcessor extends UpdateRequestProcessor {

    private final String inputFieldName;
    private final String outputField;
    private final FieldType outputFieldType;
    private final List<CopyFieldFilterFactory> filters;
    private final Analyzer analyzer;
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

    private final CallerRunsPolicy callerRuns = new CallerRunsPolicy();
    
    public XrefUpdateRequestProcessor(UpdateRequestProcessor next, SchemaField inputField, SchemaField outputField, 
        List<CopyFieldFilterFactory> filters, boolean bufferDocs) {
      super(next);
      this.inputFieldName = inputField.getName();
      this.outputField = outputField.getName();
      this.outputFieldType = PreAnalyzedField.createFieldType(outputField);
      this.filters = filters;
      this.analyzer = inputField.getType().getIndexAnalyzer();
      this.executor = new ThreadPoolExecutor(0, 20, 10, TimeUnit.SECONDS, new ArrayBlockingQueue<>(20), callerRuns);
      this.bufferDocs = bufferDocs;
    }

    @Override
    public void processAdd(AddUpdateCommand cmd) throws IOException {
      SolrInputDocument doc = cmd.getSolrInputDocument();
      Collection<Object> inputVals = doc.getFieldValues(inputFieldName);
      if (inputVals == null || inputVals.isEmpty()) {
        super.processAdd(cmd);
        return;
      }
      int size = inputVals.size();
      final boolean parallel;
      final int[] docCount;
      final Map<String, Object>[] docFields;
      Map<String, Object>[] replacements = null;
      if (exec != null) {
        parallel = true;
        docCount = new int[] {size};
        docFields = new Map[size];
        bufferedDocCount++; // the incoming doc
      } else if (size > 1 || !cmd.isLastDocInBatch) {
        parallel = true;
        docCount = new int[] {size};
        docFields = new Map[size];
        bufferedDocCount = 1; // the incoming doc
        exec = new ExecutorCompletionService<>(executor);
      } else {
        parallel = false;
        docCount = null;
        docFields = null;
        replacements = new Map[size];
      }
      int i = 0;
      for (Object val : inputVals) {
        if (parallel) {
          exec.submit(new ParallelAnalyzer(i, inputFieldName, (String)val, analyzer, outputField, outputFieldType, 
              filters, cmd, docCount, docFields));
        } else {
          replacements[i] = preAnalyze(inputFieldName, (String)val, analyzer, outputField, outputFieldType, filters);
        }
        i++;
      }
      List<Entry<AddUpdateCommand, Map<String, Object>[]>> completed;
      if (!parallel) {
        completed = Collections.singletonList(new SimpleImmutableEntry<>(cmd, replacements));
      } else {
        completed = new LinkedList<>();
        Future<ParallelAnalysisResult> future;
        try {
          while ((future = bufferDocs && !cmd.isLastDocInBatch ? exec.poll() : exec.take()) != null) {
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
        doc.removeField(inputFieldName);
        Map<String, Collection<Object>> buildValues = new HashMap<>();
        for (Map<String, Object> replacement : replacements) {
          for (Entry<String, Object> e : replacement.entrySet()) {
            String fieldName = e.getKey();
            Collection<Object> vals = buildValues.get(fieldName);
            if (vals == null) {
              vals = new ArrayList<>(size);
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
  
  private static class CFTFImpl extends CopyFieldTokenFilter {

    private final TokenTypeJoinFilter.DisplayAttribute displayAtt = getAttribute(TokenTypeJoinFilter.DisplayAttribute.class);
    private final PayloadAttribute payloadAtt = getAttribute(PayloadAttribute.class);
    private final String copyFieldDest;
    private final ArrayList<String> vals = new ArrayList<>();
    private boolean done = false;
    
    public CFTFImpl(TokenStream input, String copyFieldDest) {
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
  
  public static class CFTFImplFactory extends CopyFieldFilterFactory {

    public CFTFImplFactory(Map<String, String> args, String copyFieldDest) {
      super(args, copyFieldDest);
    }

    @Override
    public CopyFieldTokenFilter create(TokenStream ts) {
      return new CFTFImpl(ts, copyFieldDest);
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
    
    public CopyFieldFilterFactory(Map<String, String> args, String copyFieldDest) {
      super(args);
      this.copyFieldDest = copyFieldDest;
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
