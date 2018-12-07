package org.kettle.beam.core.transform;

import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.kettle.beam.core.BeamKettle;
import org.kettle.beam.core.KettleRow;
import org.kettle.beam.core.fn.StringToKettleFn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

public class BeamInputTransform extends PTransform<PBegin, PCollection<KettleRow>> {

  // These non-transient privates get serialized to spread across nodes
  //
  private String stepname;
  private String inputLocation;
  private String separator;
  private String rowMetaXml;

  // Log and count errors.
  private static final Logger LOG = LoggerFactory.getLogger( BeamInputTransform.class );
  private static final Counter numErrors = Metrics.counter( "main", "BeamInputError" );

  public BeamInputTransform() {
  }

  public BeamInputTransform( @Nullable String name, String stepname, String inputLocation, String separator, String rowMetaXml ) {
    super( name );
    this.stepname = stepname;
    this.inputLocation = inputLocation;
    this.separator = separator;
    this.rowMetaXml = rowMetaXml;
  }

  @Override public PCollection<KettleRow> expand( PBegin input ) {

    try {
      // Only initialize once on this node/vm
      //
      BeamKettle.init();

      PCollection<KettleRow> output = input

        // We read a bunch of Strings, one per line basically
        //
        .apply( stepname + " READ FILE", TextIO.read().from( inputLocation ) )

        // We need to transform these lines into Kettle fields
        //
        .apply( stepname, ParDo.of( new StringToKettleFn( rowMetaXml, separator ) ) );

      return output;

    } catch ( Exception e ) {
      numErrors.inc();
      LOG.error( "Error in beam input transform", e );
      throw new RuntimeException( "Error in beam input transform", e );
    }

  }


}
