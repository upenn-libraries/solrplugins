/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.upenn.library.solrplugins;

import java.io.IOException;
import java.util.List;
import java.util.Map.Entry;
import org.apache.commons.lang.ArrayUtils;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.FacetParams;
import org.apache.solr.common.params.ShardParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.handler.component.FacetComponent;
import static org.apache.solr.handler.component.FacetComponent.FACET_FIELD_KEY;
import static org.apache.solr.handler.component.FacetComponent.FACET_INTERVALS_KEY;
import static org.apache.solr.handler.component.FacetComponent.FACET_QUERY_KEY;
import static org.apache.solr.handler.component.FacetComponent.FACET_RANGES_KEY;
import org.apache.solr.handler.component.PivotFacetProcessor;
import org.apache.solr.handler.component.RangeFacetProcessor;
import org.apache.solr.handler.component.ResponseBuilder;
import org.apache.solr.handler.component.SpatialHeatmapFacets;
import org.apache.solr.request.SimpleFacets;
import org.apache.solr.request.XSimpleFacets;
import org.apache.solr.schema.FieldType;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.search.SyntaxError;
import org.apache.solr.search.facet.FacetDebugInfo;
import org.apache.solr.util.RTimer;

/**
 *
 * @author magibney
 */
public class XFacetComponent extends FacetComponent {

  private static final String FACET_COUNTS_ELEMENT_NAME = "facet_counts";
  private static final String FACET_FIELDS_ELEMENT_NAME = "facet_fields";

  @Override
  public void finishStage(ResponseBuilder rb) {
    super.finishStage(rb);
    if (rb.req.getParams().getBool(ShardParams.IS_SHARD, false)) {
      return;
    }
    NamedList values = rb.rsp.getValues();
    if (!FACET_COUNTS_ELEMENT_NAME.equals(values.getName(values.size() - 1))) {
      return;
    }
    NamedList<Object> facet_counts = (NamedList<Object>)values.getVal(values.size() - 1);
    NamedList<Object> facet_fields = (NamedList<Object>)facet_counts.get(FACET_FIELDS_ELEMENT_NAME);
    for (Entry<String, Object> e : facet_fields) {
      String fieldName = e.getKey();
      MultiSerializable fieldType;
      if ((fieldType = extendedFieldType(fieldName, rb)) != null) {
        fieldType.updateRepresentation((NamedList<Object>)e.getValue());
      }
    }
  }

  private MultiSerializable extendedFieldType(String name, ResponseBuilder rb) {
    IndexSchema sch;
    SchemaField sf;
    FieldType ft;
    if ((sch = rb.req.getSchema()) != null && (sf = sch.getFieldOrNull(name)) != null
        && (ft = sf.getType()) != null && ft instanceof MultiSerializable) {
      return (MultiSerializable)ft;
    } else {
      return null;
    }
  }

  /*
  *
  * Below, copied from superclass.
  *
  */
  
  private static final String PIVOT_KEY = "facet_pivot";
  
  /**
   * Actually run the query
   */
  @Override
  public void process(ResponseBuilder rb) throws IOException {

    if (rb.doFacets) {
      SolrParams params = rb.req.getParams();
      XSimpleFacets f = new XSimpleFacets(rb.req, rb.getResults().docSet, params, rb);

      RTimer timer = null;
      FacetDebugInfo fdebug = null;

      if (rb.isDebug()) {
        fdebug = new FacetDebugInfo();
        rb.req.getContext().put("FacetDebugInfo-nonJson", fdebug);
        timer = new RTimer();
      }

      NamedList<Object> counts = XFacetComponent.getFacetCounts(f, fdebug);
      String[] pivots = params.getParams(FacetParams.FACET_PIVOT);
      if (!ArrayUtils.isEmpty(pivots)) {
        PivotFacetProcessor pivotProcessor 
          = new PivotFacetProcessor(rb.req, rb.getResults().docSet, params, rb);
        SimpleOrderedMap<List<NamedList<Object>>> v 
          = pivotProcessor.process(pivots);
        if (v != null) {
          counts.add(PIVOT_KEY, v);
        }
      }

      if (fdebug != null) {
        long timeElapsed = (long) timer.getTime();
        fdebug.setElapse(timeElapsed);
      }

      rb.rsp.add("facet_counts", counts);
    }
  }

  public static NamedList<Object> getFacetCounts(XSimpleFacets simpleFacets) {
    return getFacetCounts(simpleFacets, null);
  }

  /**
   * Looks at various Params to determining if any simple Facet Constraint count
   * computations are desired.
   *
   * @see SimpleFacets#getFacetQueryCounts
   * @see SimpleFacets#getFacetFieldCounts
   * @see RangeFacetProcessor#getFacetRangeCounts
   * @see RangeFacetProcessor#getFacetIntervalCounts
   * @see FacetParams#FACET
   * @return a NamedList of Facet Count info or null
   */
  public static NamedList<Object> getFacetCounts(XSimpleFacets simpleFacets, FacetDebugInfo fdebug) {
    // if someone called this method, benefit of the doubt: assume true
    if (!simpleFacets.getGlobalParams().getBool(FacetParams.FACET, true))
      return null;

    RangeFacetProcessor rangeFacetProcessor = new RangeFacetProcessor(simpleFacets.getRequest(), simpleFacets.getDocsOrig(), simpleFacets.getGlobalParams(), simpleFacets.getResponseBuilder());
    NamedList<Object> counts = new SimpleOrderedMap<>();
    try {
      counts.add(FACET_QUERY_KEY, simpleFacets.getFacetQueryCounts());
      if (fdebug != null) {
        FacetDebugInfo fd = new FacetDebugInfo();
        fd.putInfoItem("action", "field facet");
        fd.setProcessor(simpleFacets.getClass().getSimpleName());
        fdebug.addChild(fd);
        simpleFacets.setFacetDebugInfo(fd);
        final RTimer timer = new RTimer();
        counts.add(FACET_FIELD_KEY, simpleFacets.getFacetFieldCounts());
        long timeElapsed = (long) timer.getTime();
        fd.setElapse(timeElapsed);
      } else {
        counts.add(FACET_FIELD_KEY, simpleFacets.getFacetFieldCounts());
      }
      counts.add(FACET_RANGES_KEY, rangeFacetProcessor.getFacetRangeCounts());
      counts.add(FACET_INTERVALS_KEY, simpleFacets.getFacetIntervalCounts());
      counts.add(SpatialHeatmapFacets.RESPONSE_KEY, simpleFacets.getHeatmapCounts());
    } catch (IOException e) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e);
    } catch (SyntaxError e) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, e);
    }
    return counts;
  }

}
