package org.kettle.beam.core.fn;

import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.commons.lang.StringUtils;
import org.kettle.beam.core.BeamKettle;
import org.kettle.beam.core.KettleRow;
import org.pentaho.di.core.row.RowMeta;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.xml.XMLHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

public class KettleToStringFn extends DoFn<KettleRow, String> {

  private String outputLocation;
  private String separator;
  private String enclosure;
  private String rowMetaXml;
  private List<String> stepPluginClasses;
  private List<String> xpPluginClasses;

  private transient RowMetaInterface rowMeta;
  private transient Counter readCounter;
  private transient Counter writtenCounter;

  // Log and count parse errors.
  private static final Logger LOG = LoggerFactory.getLogger( KettleToStringFn.class );
  private final Counter numParseErrors = Metrics.counter( "main", "ParseErrors" );

  public KettleToStringFn( String outputLocation, String separator, String enclosure, String rowMetaXml, List<String> stepPluginClasses, List<String> xpPluginClasses ) {
    this.outputLocation = outputLocation;
    this.separator = separator;
    this.enclosure = enclosure;
    this.rowMetaXml = rowMetaXml;
    this.stepPluginClasses = stepPluginClasses;
    this.xpPluginClasses = xpPluginClasses;
  }

  @ProcessElement
  public void processElement( ProcessContext processContext ) {
    KettleRow inputRow = processContext.element();

    try {


      if ( rowMeta == null ) {

        // Just to make sure
        //
        BeamKettle.init(stepPluginClasses, xpPluginClasses);

        rowMeta = new RowMeta( XMLHandler.getSubNode( XMLHandler.loadXMLString( rowMetaXml ), RowMeta.XML_META_TAG ) );
        readCounter = Metrics.counter( "read", "OUTPUT" );
        writtenCounter = Metrics.counter( "written", "OUTPUT");
      }

      // Just a quick and dirty output for now...
      // TODO: refine with mulitple output formats, Avro, Parquet, ...
      //
      StringBuffer line = new StringBuffer();

      for ( int i = 0; i < rowMeta.size(); i++ ) {

        if ( i > 0 ) {
          line.append( separator );
        }

        String valueString = rowMeta.getString( inputRow.getRow(), i );
        boolean enclose = false;

        if ( StringUtils.isNotEmpty( enclosure ) ) {
          enclose = valueString.contains( enclosure );
        }
        if ( enclose ) {
          line.append( enclosure );
        }
        line.append( valueString );
        if ( enclose ) {
          line.append( enclosure );
        }
      }

      // Pass the row to the process context
      //
      processContext.output( line.toString() );

    } catch ( Exception e ) {
      numParseErrors.inc();
      LOG.info( "Parse error on " + processContext.element() + ", " + e.getMessage() );
      throw new RuntimeException( "Error converting Kettle data to string lines", e );
    }
  }


}
