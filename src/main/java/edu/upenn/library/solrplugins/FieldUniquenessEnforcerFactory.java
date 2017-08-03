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

import java.io.IOException;
import java.util.ArrayDeque;
import org.apache.solr.common.SolrInputField;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.update.AddUpdateCommand;
import org.apache.solr.update.DeleteUpdateCommand;
import org.apache.solr.update.processor.UpdateRequestProcessor;
import org.apache.solr.update.processor.UpdateRequestProcessorFactory;

/**
 *
 * @author magibney
 */
public class FieldUniquenessEnforcerFactory extends UpdateRequestProcessorFactory {

  private static final String ENFORCE_UNIQUE_FIELD_ARGNAME = "enforceUniqueField";
  private static final String MAX_FIELD_VALUE_LENGTH_ARGNAME = "maxFieldValueLength";
  private static final String BUFFER_LIMIT_ARGNAME = "bufferLimit";
  private static final int DEFAULT_MAX_FIELD_VALUE_LENGTH = 30;
  private static final int DEFAULT_BUFFER_LIMIT = 100;
  private String fieldName;
  private int maxFieldValueLength;
  private int bufferLimit;

  @Override
  public void init(NamedList args) {
    this.fieldName = (String)args.get(ENFORCE_UNIQUE_FIELD_ARGNAME);
    String maxFieldValueLength = (String)args.get(MAX_FIELD_VALUE_LENGTH_ARGNAME);
    this.maxFieldValueLength = maxFieldValueLength != null ? Integer.parseInt(maxFieldValueLength) : DEFAULT_MAX_FIELD_VALUE_LENGTH;
    String bufferLimit = (String)args.get(BUFFER_LIMIT_ARGNAME);
    this.bufferLimit = bufferLimit != null ? Integer.parseInt(bufferLimit) : DEFAULT_BUFFER_LIMIT;
    super.init(args);
  }

  @Override
  public UpdateRequestProcessor getInstance(SolrQueryRequest req, SolrQueryResponse rsp, UpdateRequestProcessor next) {
    return new BufferingFieldUniquenessEnforcer(req, next, fieldName, bufferLimit, maxFieldValueLength);
  }

  private static class BufferingFieldUniquenessEnforcer extends UpdateRequestProcessor {

    private final SolrQueryRequest req;
    private final String fieldName;
    private final int threshold;
    private int nextDeleteTermsCount = 0;
    private final ArrayDeque<AddUpdateCommand> deque;
    private final StringBuilder deleteCommandBuilder;

    public BufferingFieldUniquenessEnforcer(SolrQueryRequest req, UpdateRequestProcessor next, String fieldName, int limit, int maxFieldValueLength) {
      super(next);
      this.req = req;
      this.fieldName = fieldName;
      this.threshold = limit - 1;
      this.deque = new ArrayDeque<>(limit);
      this.deleteCommandBuilder = new StringBuilder(fieldName.length() + ((limit + 1) * (maxFieldValueLength + 4)));
    }

    private StringBuilder initDeleteCommand(String fieldValue) {
      deleteCommandBuilder.setLength(0);
      nextDeleteTermsCount = 1;
      return deleteCommandBuilder.append(fieldName).append(":(").append(fieldValue);
    }

    private StringBuilder appendDelete(String fieldValue) {
      nextDeleteTermsCount++;
      return deleteCommandBuilder.append(" OR ").append(fieldValue);
    }

    private DeleteUpdateCommand finalizeDeleteComand(int commitWithin) {
      if (nextDeleteTermsCount < 1) {
        return null;
      }
      DeleteUpdateCommand delete = new DeleteUpdateCommand(req);
      delete.commitWithin = commitWithin;
      delete.setQuery(deleteCommandBuilder.append(')').toString());
      deleteCommandBuilder.setLength(0);
      nextDeleteTermsCount = 0;
      return delete;
    }

    @Override
    public void processAdd(AddUpdateCommand cmd) throws IOException {
      SolrInputField inputField = cmd.solrDoc.getField(fieldName);
      if (inputField != null) {
        String uniqueFieldValue = (String)inputField.getFirstValue();
        if (uniqueFieldValue != null) {
          if (nextDeleteTermsCount < 1) {
            initDeleteCommand(uniqueFieldValue);
          } else {
            appendDelete(uniqueFieldValue);
          }
        }
      }
      if (cmd.isLastDocInBatch || deque.size() >= threshold) {
        DeleteUpdateCommand delete = finalizeDeleteComand(cmd.commitWithin);
        if (delete != null) {
          super.processDelete(delete);
        }
        AddUpdateCommand flush;
        while ((flush = deque.poll()) != null) {
          super.processAdd(flush);
        }
        super.processAdd(cmd);
      } else {
        deque.add(cmd);
      }
    }
  }
}
