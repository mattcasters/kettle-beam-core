package org.kettle.beam.core.fn;

import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;
import org.kettle.beam.core.BeamKettle;
import org.kettle.beam.core.KettleRow;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleValueException;
import org.pentaho.di.core.row.RowDataUtil;
import org.pentaho.di.core.row.RowMeta;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.row.value.ValueMetaString;
import org.pentaho.di.core.xml.XMLHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StringToKettleFn extends DoFn<String, KettleRow> {

  private String rowMetaXml;
  private String separator;

  private transient Counter readCounter;
  private transient Counter writtenCounter;

  // Log and count parse errors.
  private static final Logger LOG = LoggerFactory.getLogger( StringToKettleFn.class );
  private final Counter numParseErrors = Metrics.counter( "main", "ParseErrors" );

  private transient RowMetaInterface rowMeta;

  public StringToKettleFn( String rowMetaXml, String separator) {
    this.rowMetaXml = rowMetaXml;
    this.separator = separator;
  }

  @ProcessElement
  public void processElement( ProcessContext processContext ) {

    String inputString = processContext.element();

    String[] components = inputString.split( separator, -1 );

    // TODO: implement enclosure in FileDefinition
    //
    try {

      // Just to make sure
      BeamKettle.init();

      if ( rowMeta == null ) {
        rowMeta = new RowMeta( XMLHandler.getSubNode(XMLHandler.loadXMLFile( rowMetaXml ), RowMeta.XML_META_TAG) );
        readCounter = Metrics.counter( "read", "INPUT");
        writtenCounter = Metrics.counter( "written", "INPUT");
      }

      Object[] row = RowDataUtil.allocateRowData( rowMeta.size() );
      int index = 0;
      while ( index < rowMeta.size() && index < components.length ) {
        String sourceString = components[ index ];
        ValueMetaInterface valueMeta = rowMeta.getValueMeta( index );
        ValueMetaInterface stringMeta = new ValueMetaString( "SourceString" );
        stringMeta.setConversionMask( valueMeta.getConversionMask() );
        try {
          row[ index ] = valueMeta.convertDataFromString( sourceString, stringMeta, null, null, ValueMetaInterface.TRIM_TYPE_NONE );
        } catch ( KettleValueException ve ) {
          throw new KettleException( "Unable to convert value '" + sourceString + "' to value : " + valueMeta.toStringMeta(), ve );
        }
        index++;
      }

      // Pass the row to the process context
      //
      processContext.output( new KettleRow( row ) );

    } catch ( Exception e ) {
      e.printStackTrace();
      // Failure failure = new Failure(StringToKettleFn.class.getName(), Const.getStackTracker(e), inputString);
      // processContext.output( "failure", failure );

      numParseErrors.inc();
      LOG.error( "Parse error on " + processContext.element() + ", " + e.getMessage() );
    }
  }


}
