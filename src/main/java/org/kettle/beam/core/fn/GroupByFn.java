package org.kettle.beam.core.fn;

import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.kettle.beam.core.KettleRow;
import org.kettle.beam.core.shared.AggregationType;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.row.RowDataUtil;
import org.pentaho.di.core.row.RowMeta;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.xml.XMLHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GroupByFn extends DoFn<KV<KettleRow, Iterable<KettleRow>>, KettleRow> {


  private String groupRowMetaXml; // The data types of the subjects
  private String subjectRowMetaXml; // The data types of the subjects
  private String[] aggregations; // The aggregation types

  private static final Logger LOG = LoggerFactory.getLogger( GroupByFn.class );
  private final Counter numErrors = Metrics.counter( "main", "GroupByErrors" );

  private transient RowMetaInterface groupRowMeta;
  private transient RowMetaInterface subjectRowMeta;

  private transient AggregationType[] aggregationTypes = null;

  public GroupByFn() {
  }

  public GroupByFn( String groupRowMetaXml, String subjectRowMetaXml, String[] aggregations ) {
    this.groupRowMetaXml = groupRowMetaXml;
    this.subjectRowMetaXml = subjectRowMetaXml;
    this.aggregations = aggregations;
  }

  @ProcessElement
  public void processElement( ProcessContext processContext ) {

    try {
      if ( aggregationTypes == null ) {

        groupRowMeta = new RowMeta( XMLHandler.getSubNode( XMLHandler.loadXMLString( groupRowMetaXml ), RowMeta.XML_META_TAG ) );
        subjectRowMeta = new RowMeta( XMLHandler.getSubNode( XMLHandler.loadXMLString( subjectRowMetaXml ), RowMeta.XML_META_TAG ) );

        aggregationTypes = new AggregationType[aggregations.length];
        for ( int i = 0; i < aggregationTypes.length; i++ ) {
          aggregationTypes[ i ] = AggregationType.getTypeFromName( aggregations[ i ] );
        }
      }

      KV<KettleRow, Iterable<KettleRow>> inputElement = processContext.element();
      KettleRow groupKettleRow = inputElement.getKey();
      Object[] groupRow = groupKettleRow.getRow();

      // Initialize the aggregation results for this window
      //
      Object[] results = new Object[aggregationTypes.length];
      for (int i=0;i<results.length;i++) {
        results[i] = null;
      }

      Iterable<KettleRow> subjectKettleRows = inputElement.getValue();
      for ( KettleRow subjectKettleRow : subjectKettleRows ) {
        Object[] subjectRow = subjectKettleRow.getRow();

        // Aggregate this...
        //
        for (int i=0;i<aggregationTypes.length;i++) {
          ValueMetaInterface subjectValueMeta = subjectRowMeta.getValueMeta( i );
          Object subject = subjectRow[i];
          Object result = results[i];

          switch(aggregationTypes[i]) {
            case SUM: {
              if ( result == null ) {
                result = subject;
              } else {
                switch ( subjectValueMeta.getType() ) {
                  case ValueMetaInterface.TYPE_INTEGER:
                    result = (Long) result + (Long) subject;
                    break;
                  case ValueMetaInterface.TYPE_NUMBER:
                    result = (Double)result + (Double)subject;
                    break;
                  default:
                    throw new KettleException( "SUM aggregation not yet implemented for field and data type : "+subjectValueMeta.toString() );
                  }
                }
              }
              break;
            case COUNT:
              if (subject!=null) {
                if (result==null){
                  result = Long.valueOf( 1L );
                } else {
                  result = (Long)result + 1L;
                }
              }
              break;
            default:
              throw new KettleException( "Matt is very lazy and hasn't implemented this aggregation type yet: "+aggregationTypes[i].name() );

          }
          results[i] = result;
        }

      }

      // Now we have the results
      // Concatenate both group and result...
      //
      Object[] resultRow = RowDataUtil.allocateRowData( groupRowMeta.size()+subjectRowMeta.size() );
      int index = 0;
      for (int i=0;i<groupRowMeta.size();i++) {
        resultRow[index++] = groupRow[i];
      }
      for (int i=0;i<subjectRowMeta.size();i++) {
        resultRow[index++] = results[i];
      }

      // Send it on its way
      //
      processContext.output( new KettleRow( resultRow ) );

    } catch(Exception e) {
      e.printStackTrace();
      numErrors.inc();
      LOG.error("Error grouping by ", e);
      throw new RuntimeException( "Unable to split row into group and subject ", e );
    }


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
}
