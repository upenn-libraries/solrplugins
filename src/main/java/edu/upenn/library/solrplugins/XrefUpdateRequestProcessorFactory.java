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
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.core.KeywordTokenizer;
import org.apache.lucene.analysis.tokenattributes.PayloadAttribute;
import org.apache.lucene.analysis.util.TokenFilterFactory;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
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

  private static final JsonPreAnalyzedParser parser = new JsonPreAnalyzedParser();
  private static final String INPUT_FIELD_ARGNAME = "inputField";
  private static final String OUTPUT_FIELD_ARGNAME = "outputField";
  private static final String COPY_FIELD_DEST_ARGNAME = "copyFieldDest";

  private String inputField;
  private String outputField;
  private String copyFieldDest;
  
  @Override
  public UpdateRequestProcessor getInstance(SolrQueryRequest req, SolrQueryResponse rsp, UpdateRequestProcessor next) {
    IndexSchema schema = req.getSchema();
    return new XrefUpdateRequestProcessor(next, schema.getField(inputField), schema.getField(outputField), Collections.singletonList(new BlahFactory(Collections.EMPTY_MAP, copyFieldDest)));
  }

  @Override
  public void init(NamedList args) {
    this.inputField = (String) args.get(INPUT_FIELD_ARGNAME);
    this.outputField = (String) args.get(OUTPUT_FIELD_ARGNAME);
    this.copyFieldDest = (String) args.get(COPY_FIELD_DEST_ARGNAME);
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
        Collections.singletonList(new BlahFactory(Collections.EMPTY_MAP, "copyFieldName")));
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

    public XrefUpdateRequestProcessor(UpdateRequestProcessor next, SchemaField inputField, SchemaField outputField, 
        List<CopyFieldFilterFactory> filters) {
      super(next);
      this.inputFieldName = inputField.getName();
      this.outputField = outputField.getName();
      this.outputFieldType = PreAnalyzedField.createFieldType(outputField);
      this.filters = filters;
      this.analyzer = inputField.getType().getIndexAnalyzer();
      this.executor = Executors.newCachedThreadPool();
    }

    @Override
    public void processAdd(AddUpdateCommand cmd) throws IOException {
      SolrInputDocument doc = cmd.getSolrInputDocument();
      Collection<Object> inputVals = doc.getFieldValues(inputFieldName);
      if (inputVals == null) {
        super.processAdd(cmd);
        return;
      }
      int size = inputVals.size();
      final boolean parallel = size > 1;
      ExecutorCompletionService<Entry<Integer, Map<String, Object>>> exec = parallel ? new ExecutorCompletionService<>(executor) : null;
      Map<String, Object>[] replacements = new Map[size];
      int i = 0;
      for (Object val : inputVals) {
        if (parallel) {
          exec.submit(new ParallelAnalyzer(i, inputFieldName, (String)val, analyzer, outputField, outputFieldType, filters));
        } else {
          replacements[i] = preAnalyze(inputFieldName, (String)val, analyzer, outputField, outputFieldType, filters);
        }
        i++;
      }
      if (parallel) {
        Entry<Integer, Map<String, Object>>[] tmp = new Entry[size];
        for (i = 0; i < size; i++) {
          try {
            Future<Entry<Integer, Map<String, Object>>> result = exec.poll(30, TimeUnit.SECONDS);
            tmp[i] = result.get();
          } catch (InterruptedException | ExecutionException ex) {
            throw new RuntimeException(ex);
          }
        }
        Arrays.sort(tmp, 0, size, ENTRY_COMPARATOR);
        for (i = 0; i < size; i++) {
          replacements[i] = tmp[i].getValue();
        }
      }
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
  
  private static final Comparator ENTRY_COMPARATOR = Entry.comparingByKey();

  private static class Blah extends CopyFieldTokenFilter {

    private final TokenTypeJoinFilter.DisplayAttribute displayAtt = getAttribute(TokenTypeJoinFilter.DisplayAttribute.class);
    private final PayloadAttribute payloadAtt = getAttribute(PayloadAttribute.class);
    private final String copyFieldDest;
    private final ArrayList<String> vals = new ArrayList<>();
    private boolean done = false;
    
    public Blah(TokenStream input, String copyFieldDest) {
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
  
  public static class BlahFactory extends CopyFieldFilterFactory {

    public BlahFactory(Map<String, String> args, String copyFieldDest) {
      super(args, copyFieldDest);
    }

    @Override
    public CopyFieldTokenFilter create(TokenStream ts) {
      return new Blah(ts, copyFieldDest);
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

  private static class ParallelAnalyzer implements Callable<Entry<Integer, Map<String, Object>>> {

    private final int index;
    private final String fieldName;
    private final String input;
    private final Analyzer analyzer;
    private final String outputFieldName;
    private final FieldType outputFieldType;
    private final List<CopyFieldFilterFactory> filters;
    
    public ParallelAnalyzer(int index, String fieldName, String input, Analyzer analyzer, String outputFieldName, 
        FieldType outputFieldType, List<CopyFieldFilterFactory> filters) {
      this.index = index;
      this.fieldName = fieldName;
      this.input = input;
      this.analyzer = analyzer;
      this.outputFieldName = outputFieldName;
      this.outputFieldType = outputFieldType;
      this.filters = filters;
    }
    
    @Override
    public Entry<Integer, Map<String, Object>> call() {
      try {
        return new SimpleImmutableEntry<>(index, preAnalyze(fieldName, input, analyzer, outputFieldName, outputFieldType, filters));
      } catch (IOException ex) {
        ex.printStackTrace(System.err);
        throw new RuntimeException(ex);
      }
    }

  }
}
