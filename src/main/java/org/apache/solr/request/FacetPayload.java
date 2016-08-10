/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package org.apache.solr.request;

import java.io.IOException;
import org.apache.lucene.index.PostingsEnum;
import org.apache.solr.common.util.NamedList;

/**
 *
 * @author magibney
 */
public interface FacetPayload {
  boolean addEntry(String value, int count, PostingsEnum postings, NamedList res) throws IOException;
  NamedList<Object> mergePayload(NamedList<Object> preExisting, NamedList<Object> add);
}
