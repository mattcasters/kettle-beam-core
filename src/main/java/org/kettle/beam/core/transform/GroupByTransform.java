package org.kettle.beam.core.transform;

import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.CombineFnBase;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.Max;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.kettle.beam.core.BeamKettle;
import org.kettle.beam.core.KettleRow;
import org.kettle.beam.core.fn.GroupByFn;
import org.kettle.beam.core.fn.GroupSubjectFn;
import org.pentaho.di.core.row.RowMeta;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.xml.XMLHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GroupByTransform extends PTransform<PCollection<KettleRow>, PCollection<KettleRow>> {

  // The non-transient methods are serializing
  // Keep them simple to stay out of trouble.
  //
  private String rowMetaXml;   // The input row
  private String[] groupFields;  // The fields to group over
  private String[] subjects; // The subjects to aggregate on
  private String[] aggregations; // The aggregation types
  private String[] resultFields; // The result fields

  private static final Logger LOG = LoggerFactory.getLogger( GroupByTransform.class );
  private final Counter numErrors = Metrics.counter( "main", "GroupByTransformErrors" );

  private transient RowMetaInterface inputRowMeta;
  private transient RowMetaInterface groupRowMeta;
  private transient RowMetaInterface subjectRowMeta;

  public GroupByTransform() {
  }

  public GroupByTransform( String rowMetaXml, String[] groupFields, String[] subjects, String[] aggregations, String[] resultFields) {
    this.rowMetaXml = rowMetaXml;
    this.groupFields = groupFields;
    this.subjects = subjects;
    this.aggregations = aggregations;
    this.resultFields = resultFields;
  }

  @Override public PCollection<KettleRow> expand( PCollection<KettleRow> input ) {

    try {
      if ( inputRowMeta == null ) {
        BeamKettle.init();

        inputRowMeta = new RowMeta( XMLHandler.getSubNode( XMLHandler.loadXMLString( rowMetaXml ), RowMeta.XML_META_TAG ) );

        groupRowMeta = new RowMeta();
        for (int i=0;i<groupFields.length;i++) {
          groupRowMeta.addValueMeta( inputRowMeta.searchValueMeta( groupFields[i] ) );
        }
        subjectRowMeta = new RowMeta();
        for (int i=0;i<subjects.length;i++) {
          subjectRowMeta.addValueMeta( inputRowMeta.searchValueMeta( subjects[i] ) );
        }

      }

      // Split the KettleRow into GroupFields-KettleRow and SubjectFields-KettleRow
      //
      PCollection<KV<KettleRow, KettleRow>> groupSubjects = input.apply( ParDo.of( new GroupSubjectFn( rowMetaXml, groupFields, subjects ) ) );

      // Now we need to aggregate the groups with a Combine
      GroupByKey<KettleRow, KettleRow> byKey = GroupByKey.<KettleRow, KettleRow>create();
      PCollection<KV<KettleRow, Iterable<KettleRow>>> grouped = groupSubjects.apply( byKey );

      // Aggregate the rows in the grouped PCollection
      //   Input: KV<KettleRow>, Iterable<KettleRow>>
      //   This means that The group rows is in KettleRow.  For every one of these, you get a list of subject rows.
      //   We need to calcualte the aggregation of these subject lists
      //   Then we output group values with result values behind it.
      //
      PCollection<KettleRow> output = grouped.apply( ParDo.of( new GroupByFn( groupRowMeta.getMetaXML(), subjectRowMeta.getMetaXML(), aggregations ) ) );

      return output;
    } catch(Exception e) {
      e.printStackTrace();
      numErrors.inc();
      LOG.error( "Error in group by transform", e );
      throw new RuntimeException( "Error in group by transform", e );
    }
  }


  /**
   * Gets rowMetaXml
   *
   * @return value of rowMetaXml
   */
  public String getRowMetaXml() {
    return rowMetaXml;
  }

  /**
   * @param rowMetaXml The rowMetaXml to set
   */
  public void setRowMetaXml( String rowMetaXml ) {
    this.rowMetaXml = rowMetaXml;
  }

  /**
   * Gets groupFields
   *
   * @return value of groupFields
   */
  public String[] getGroupFields() {
    return groupFields;
  }

  /**
   * @param groupFields The groupFields to set
   */
  public void setGroupFields( String[] groupFields ) {
    this.groupFields = groupFields;
  }

  /**
   * Gets subjects
   *
   * @return value of subjects
   */
  public String[] getSubjects() {
    return subjects;
  }

  /**
   * @param subjects The subjects to set
   */
  public void setSubjects( String[] subjects ) {
    this.subjects = subjects;
  }

  /**
   * Gets aggregations
   *
   * @return value of aggregations
   */
  public String[] getAggregations() {
    return aggregations;
  }

  /**
   * @param aggregations The aggregations to set
   */
  public void setAggregations( String[] aggregations ) {
    this.aggregations = aggregations;
  }

  /**
   * Gets resultFields
   *
   * @return value of resultFields
   */
  public String[] getResultFields() {
    return resultFields;
  }

  /**
   * @param resultFields The resultFields to set
   */
  public void setResultFields( String[] resultFields ) {
    this.resultFields = resultFields;
  }
}
