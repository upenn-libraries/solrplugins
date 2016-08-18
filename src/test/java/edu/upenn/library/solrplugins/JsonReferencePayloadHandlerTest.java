package edu.upenn.library.solrplugins;

import static junit.framework.Assert.assertEquals;
import org.apache.solr.common.util.NamedList;
import org.junit.Test;

public class JsonReferencePayloadHandlerTest {

  @Test
  public void testMergePayload() {
    NamedList<Object> preUseForTargetCounts = new NamedList<>();
    preUseForTargetCounts.add("G. Hegel", 2L);
    preUseForTargetCounts.add("Georg Hegel", 3L);

    NamedList<Object> preExisting = new NamedList<>();
    preExisting.add("count", 10L);
    preExisting.add("use_for", preUseForTargetCounts);

    NamedList<Object> addUseForTargetCounts = new NamedList<>();
    addUseForTargetCounts.add("G. Hegel", 4L);
    addUseForTargetCounts.add("Hegel", 1L);

    NamedList<Object> addSeeAlsoTargetCounts = new NamedList<>();
    addSeeAlsoTargetCounts.add("G. W. F. Hegel", 4L);

    NamedList<Object> add = new NamedList<>();
    add.add("count", 4L);
    add.add("use_for", addUseForTargetCounts);
    add.add("see_also", addSeeAlsoTargetCounts);

    JsonReferencePayloadHandler handler = new JsonReferencePayloadHandler();

    NamedList<Object> result = (NamedList<Object>) handler.mergePayload(preExisting, add, 10L, 4L);

    assertEquals(14L, result.get("count"));

    NamedList<Object> useForTargetCounts = (NamedList<Object>) result.get("use_for");

    assertEquals(3, useForTargetCounts.size());

    assertEquals(6L, useForTargetCounts.get("G. Hegel"));
    assertEquals(3L, useForTargetCounts.get("Georg Hegel"));
    assertEquals(1L, useForTargetCounts.get("Hegel"));

    NamedList<Object> seeAlsoTargetCounts = (NamedList<Object>) result.get("see_also");

    assertEquals(1, seeAlsoTargetCounts.size());

    assertEquals(4L, seeAlsoTargetCounts.get("G. W. F. Hegel"));
  }

}
