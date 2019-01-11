package org.kettle.beam.core.fn;

import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.commons.lang.StringUtils;
import org.joda.time.Instant;
import org.kettle.beam.core.BeamKettle;
import org.kettle.beam.core.KettleRow;
import org.kettle.beam.core.util.JsonRowMeta;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.row.value.ValueMetaTimestamp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.List;

public class TimestampFn extends DoFn<KettleRow, KettleRow> {

  private String stepname;
  private String rowMetaJson;
  private String fieldName;
  private List<String> stepPluginClasses;
  private List<String> xpPluginClasses;

  private transient Counter initCounter;
  private transient Counter readCounter;
  private transient Counter writtenCounter;
  private transient Counter errorCounter;

  private transient int fieldIndex;

  // Log and count parse errors.
  private static final Logger LOG = LoggerFactory.getLogger( TimestampFn.class );

  private transient RowMetaInterface rowMeta;
  private transient ValueMetaInterface fieldValueMeta;

  public TimestampFn( String stepname, String rowMetaJson, String fieldName, List<String> stepPluginClasses, List<String> xpPluginClasses ) {
    this.stepname = stepname;
    this.rowMetaJson = rowMetaJson;
    this.fieldName = fieldName;
    this.stepPluginClasses = stepPluginClasses;
    this.xpPluginClasses = xpPluginClasses;
  }

  @ProcessElement
  public void processElement( ProcessContext processContext ) {

    try {

      if ( rowMeta == null ) {

        // Just to make sure
        //
        BeamKettle.init( stepPluginClasses, xpPluginClasses );

        rowMeta = JsonRowMeta.fromJson( rowMetaJson );

        initCounter = Metrics.counter( "init", stepname );
        readCounter = Metrics.counter( "read", stepname );
        writtenCounter = Metrics.counter( "written", stepname );
        errorCounter = Metrics.counter( "error", stepname );

        fieldIndex = -1;
        if (StringUtils.isNotEmpty( fieldName )) {
          fieldIndex = rowMeta.indexOfValue( fieldName );
          if (fieldIndex<0) {
            throw new RuntimeException( "Field '"+fieldName+"' couldn't be found in put : "+rowMeta.toString() );
          }
          fieldValueMeta = rowMeta.getValueMeta( fieldIndex );
        }

        initCounter.inc();
      }

      KettleRow inputString = processContext.element();
      readCounter.inc();

      Instant instant;
      if (fieldIndex<0) {
        instant = Instant.now();
      } else {
        Object fieldData = inputString.getRow()[fieldIndex];
        if (ValueMetaInterface.TYPE_TIMESTAMP==fieldValueMeta.getType()) {
          java.sql.Timestamp timestamp = ( (ValueMetaTimestamp) fieldValueMeta ).getTimestamp( fieldData );
          instant = new Instant(timestamp.toInstant());
        } else {
          Date date = fieldValueMeta.getDate( fieldData );
          instant = new Instant(date.toInstant());
        }
      }

      // Pass the row to the process context
      //
      processContext.outputWithTimestamp( inputString, instant );
      writtenCounter.inc();

    } catch ( Exception e ) {
      errorCounter.inc();
      LOG.error( "Error adding timestamp to rows : " + processContext.element() + ", " + e.getMessage() );
      throw new RuntimeException( "Error adding timestamp to rows", e );
    }
  }


}
