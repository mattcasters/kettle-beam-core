package org.kettle.beam.core.fn;

import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.kettle.beam.core.BeamKettle;
import org.kettle.beam.core.KettleRow;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.row.RowDataUtil;
import org.pentaho.di.core.row.RowMeta;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.xml.XMLHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// Split a row into group and subject parts
//
public class GroupSubjectFn extends DoFn<KettleRow, KV<KettleRow, KettleRow>> {

  private String inputRowMetaXml;
  private String[] groupFields;
  private String[] subjectFields;

  private static final Logger LOG = LoggerFactory.getLogger( GroupSubjectFn.class );
  private final Counter numErrors = Metrics.counter( "main", "GroupSubjectErrors" );


  // private String[] aggregations; // The aggregation types
  // private String[] resultFields; // The result fields

  private transient RowMetaInterface inputRowMeta;
  // private transient RowMetaInterface groupRowMeta;
  private transient int[] groupIndexes;
  // private transient RowMetaInterface resultRowMeta;
  private transient int[] subjectIndexes;
  // private transient AggregationType[] aggregationTypes;

  public GroupSubjectFn() {
  }

  public GroupSubjectFn( String inputRowMetaXml, String[] groupFields, String[] subjectFields /*, String[] aggregations, String[] resultFields */ ) {
    this.inputRowMetaXml = inputRowMetaXml;
    this.groupFields = groupFields;
    this.subjectFields = subjectFields;
    // this.aggregations = aggregations;
    // this.resultFields = resultFields;
  }

  @ProcessElement
  public void processElement( ProcessContext processContext ) {

    try {

      BeamKettle.init();

      if ( inputRowMeta == null ) {
        // Initialize
        //
        inputRowMeta = new RowMeta( XMLHandler.getSubNode( XMLHandler.loadXMLString( inputRowMetaXml ), RowMeta.XML_META_TAG ) );

        // Construct the group row metadata
        //
        // groupRowMeta = new RowMeta();
        groupIndexes = new int[groupFields.length];
        for ( int i=0;i<groupFields.length;i++) {
          groupIndexes[i]=inputRowMeta.indexOfValue( groupFields[i] );
          if (groupIndexes[i]<0) {
            throw new KettleException( "Unable to find group by field '"+groupFields[i]+"' in input "+inputRowMeta.toString() );
          }
          // groupRowMeta.addValueMeta( inputRowMeta.getValueMeta( groupIndexes[i] ).clone() );
        }

        // resultRowMeta = new RowMeta();
        subjectIndexes=new int[subjectFields.length];
        // aggregationTypes=new AggregationType[subjectFields.length];
        for (int i=0;i<subjectFields.length;i++) {
          subjectIndexes[i] = inputRowMeta.indexOfValue( subjectFields[i] );
          // aggregationTypes[i] = AggregationType.getTypeFromName( aggregations[i] );
          /*
          ValueMetaInterface subjectValueMeta = inputRowMeta.getValueMeta( subjectIndexes[i] );
          switch(aggregationTypes[i]) {
            case COUNT_ALL:
              // Count always gives back a Long
              //
              resultRowMeta.addValueMeta( new ValueMetaInteger( resultFields[i] ) );
              break;
            case SUM:
            case MIN:
            case MAX:
              // These give back the same data type as the subject...
              //
              resultRowMeta.addValueMeta( subjectValueMeta.clone() );
                break;
            default:
              throw new KettleException( "Unsupported aggregation type : "+aggregations[i] );
          }
          */
        }

        // Now that we know everything, we can split the row...
        //
      }

      // Get an input row
      //
      KettleRow inputKettleRow = processContext.element();
      Object[] inputRow = inputKettleRow.getRow();

      // Copy over the data...
      //
      Object[] groupRow = RowDataUtil.allocateRowData( groupIndexes.length );
      for (int i=0;i<groupIndexes.length;i++) {
        groupRow[i] = inputRow[groupIndexes[i]];
      }

      // Copy over the subjects...
      //
      Object[] subjectRow = RowDataUtil.allocateRowData( subjectIndexes.length );
      for (int i=0;i<subjectIndexes.length;i++) {
        subjectRow[i] = inputRow[subjectIndexes[i]];
      }

      KV<KettleRow, KettleRow> keyValue = KV.of( new KettleRow(groupRow), new KettleRow( subjectRow ) );
      processContext.output( keyValue );


    } catch(Exception e) {
      numErrors.inc();
      LOG.error("Error splitting row into group and subject", e);
      throw new RuntimeException( "Unable to split row into group and subject ", e );
    }
  }


  /**
   * Gets inputRowMetaXml
   *
   * @return value of inputRowMetaXml
   */
  public String getInputRowMetaXml() {
    return inputRowMetaXml;
  }

  /**
   * @param inputRowMetaXml The inputRowMetaXml to set
   */
  public void setInputRowMetaXml( String inputRowMetaXml ) {
    this.inputRowMetaXml = inputRowMetaXml;
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
   * Gets subjectFields
   *
   * @return value of subjectFields
   */
  public String[] getSubjectFields() {
    return subjectFields;
  }

  /**
   * @param subjectFields The subjectFields to set
   */
  public void setSubjectFields( String[] subjectFields ) {
    this.subjectFields = subjectFields;
  }


}
