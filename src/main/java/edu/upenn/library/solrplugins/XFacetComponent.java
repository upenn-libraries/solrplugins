/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.upenn.library.solrplugins;

import java.util.Map.Entry;
import org.apache.solr.common.params.ShardParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.handler.component.FacetComponent;
import org.apache.solr.handler.component.ResponseBuilder;
import org.apache.solr.schema.FieldType;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.schema.SchemaField;

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

}
