/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.solr.request;

import java.io.IOException;
import java.util.AbstractMap;
import java.util.Arrays;
import java.util.Deque;
import java.util.Map;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.request.BidirectionalFacetResponseBuilder.AscendingFacetTermIteratorFactory;
import org.apache.solr.request.BidirectionalFacetResponseBuilder.DescendingFacetTermIteratorFactory;
import org.apache.solr.request.BidirectionalFacetResponseBuilder.Env;
import org.apache.solr.request.BidirectionalFacetResponseBuilder.SimpleTermIndexKey;
import org.apache.solr.schema.FieldType;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 *
 * @author magibney
 */
public class BidirectionalFacetResponseBuilderTest<T extends FieldType & FacetPayload> {
  
  private final int[] counts;
  private final int[] evenCounts;
  private final int[] oddCounts;
  
  public BidirectionalFacetResponseBuilderTest() {
    counts = new int[3];
    Arrays.fill(counts, 1);
    evenCounts = new int[5];
    oddCounts = new int[5];
    for (int i = 0; i < 5; i++) {
      switch (i % 2) {
        case 0:
          evenCounts[i] = 1;
          break;
        case 1:
          oddCounts[i] = 1;
          break;
      }
    }
  }
  
  @Test
  public void testZeroOffset() throws IOException {
    runTest(1, 0, 0, 0, 0);
    runTest(oddCounts, 1, 0, 0, 0, 1);
    runTest(evenCounts, 1, 0, 0, 0, 0);
    runTest(1, 1, 0, 0, 1);
    runTest(oddCounts, 1, 1, 0, 0, 1);
    runTest(evenCounts, 1, 1, 0, 0, 2);
  }
  
  @Test
  public void testNegativeOffset() throws IOException {
    runTest(1, 1, -1, -1, 2);
  }
  
  @Test
  public void testLargeNegativeOffset() throws IOException {
    runTest(1, 1, -2, -1, 2);
  }
  
  @Test
  public void testPositiveOffset() throws IOException {
    runTest(1, 1, 1, 1, 0);
  }
  
  @Test
  public void testLargePositiveOffset() throws IOException {
    runTest(1, 1, 2, 1, 0);
  }
  
  @Test
  public void testZeroOffsetWindowOOB() throws IOException {
    runTest(1, 3, 0, 1, 2);
  }
  
  @Test
  public void testNegativeOffsetWindowOOB() throws IOException {
    runTest(1, 2, -1, 0, 2);
    runTest(1, 3, -1, 1, 2);
  }
  
  @Test
  public void testLargeNegativeOffsetWindowOOB() throws IOException {
    runTest(1, 1, -2, -1, 2);
  }
  
  @Test
  public void testPositiveOffsetWindowOOB() throws IOException {
    runTest(1, 0, 1, 0, 0);
  }
  
  @Test
  public void testLargePositiveOffsetWindowOOB() throws IOException {
    runTest(1, 0, 2, 0, 0);
    runTest(1, 1, 2, 1, 0);
  }
  
  private void runTest(int limit, int targetIdx, int requestedOffset, Integer expectedOffset, int... expectedIndices) throws IOException {
    runTest(counts, limit, targetIdx, requestedOffset, expectedOffset, expectedIndices);
  }

  private void runTest(int[] counts, int limit, int targetIdx, int requestedOffset, Integer expectedOffset, int... expectedIndices) throws IOException {
    String targetDoc = null;
    int mincount = 1;
    String fieldName = "myField";
    T ft = null;
    NamedList exp = buildExpected(expectedOffset, expectedIndices);
    NamedList actual = new NamedList(3);
    Env<T, SimpleTermIndexKey> env = new TestEnv<>(requestedOffset, limit, targetIdx, targetDoc, mincount, fieldName, ft, actual, counts);
    BidirectionalFacetResponseBuilder.build(env, new DescendingFacetTermIteratorFactory(),
              new AscendingFacetTermIteratorFactory());
    assertEquals(actual.get("count"), ((NamedList)actual.get("terms")).size());
    assertEquals(exp, actual);
  }
  
  private NamedList buildExpected(Integer expectedTargetOffset, int... expected) {
    NamedList ret = new NamedList(3);
    ret.add("count", expected.length);
    if (expectedTargetOffset != null) {
      ret.add("target_offset", expectedTargetOffset);
    }
    NamedList terms = new NamedList(expected.length);
    ret.add("terms", terms);
    for (int i : expected) {
      terms.add(Integer.toString(i), 1);
    }
    return ret;
  }
  
  private static class TestEnv<T extends FieldType & FacetPayload> extends Env<T, SimpleTermIndexKey> {

    private final int[] counts;
    
    public TestEnv(int offset, int limit, int targetIdx, String targetDoc, int mincount, String fieldName, T ft, NamedList res, int[] counts) {
      super(offset, limit, targetIdx, targetDoc, mincount, fieldName, ft, res);
      this.counts = counts;
    }
    
    @Override
    public SimpleTermIndexKey incrementKey(SimpleTermIndexKey previousKey) {
      int index = previousKey.index;
      index = index < 0 ? 0 : index + 1;
      for (; index < counts.length; index++) {
        if (acceptTerm(index)) {
          return new SimpleTermIndexKey(index);
        }
      }
      return null;
    }

    @Override
    public SimpleTermIndexKey decrementKey(SimpleTermIndexKey previousKey) {
      int index = previousKey.index;
      index = (index > counts.length ? counts.length : index) - 1;
      for (; index >= 0; index--) {
        if (acceptTerm(index)) {
          return new SimpleTermIndexKey(index);
        }
      }
      return null;
    }

    @Override
    public void addEntry(BidirectionalFacetResponseBuilder.LimitMinder<T, SimpleTermIndexKey> limitMinder, SimpleTermIndexKey facetKey, Deque<Map.Entry<String, Object>> entryBuilder) throws IOException {
      int index = facetKey.index;
      limitMinder.addEntry(new AbstractMap.SimpleImmutableEntry<>(Integer.toString(index), counts[index]), entryBuilder);
    }

    @Override
    public SimpleTermIndexKey targetKey() throws IOException {
      int index = (targetIdx < 0 ? ~targetIdx : targetIdx);
      return new SimpleTermIndexKey(index);
    }

    @Override
    public SimpleTermIndexKey targetKeyInit(boolean ascending) throws IOException {
      int index = (targetIdx < 0 ? ~targetIdx : targetIdx);
      SimpleTermIndexKey ret = new SimpleTermIndexKey(index);
      if (index >= 0 && index < counts.length && acceptTerm(index)) {
        return ret;
      } else if (ascending) {
        return incrementKey(ret);
      } else {
        return decrementKey(ret);
      }
    }

    private boolean acceptTerm(int i) {
      return counts[i] >= mincount;
    }

  }
  
}
