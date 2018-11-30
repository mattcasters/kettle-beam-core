package org.kettle.beam.core.transform;

import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.kettle.beam.core.BeamKettle;
import org.kettle.beam.core.KettleRow;
import org.kettle.beam.core.fn.StepFn;
import org.kettle.beam.core.shared.VariableValue;
import org.pentaho.di.core.exception.KettleException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class StepTransform  extends PTransform<PCollection<KettleRow>, PCollection<KettleRow>> {

  protected List<VariableValue> variableValues;
  protected String stepname;
  protected String stepPluginId;
  protected String inputRowMetaXml;
  protected String stepMetaInterfaceXml;

  // Log and count errors.
  private static final Logger LOG = LoggerFactory.getLogger( StepTransform.class );
  private static final Counter numErrors = Metrics.counter( "main", "StepErrors" );

  public StepTransform() {
    variableValues = new ArrayList<>();
  }

  public StepTransform( List<VariableValue> variableValues, String stepname, String stepPluginId, String stepMetaInterfaceXml, String inputRowMetaXml) throws KettleException, IOException {
    this.variableValues = variableValues;
    this.stepname = stepname;
    this.stepPluginId = stepPluginId;
    this.stepMetaInterfaceXml = stepMetaInterfaceXml;
    this.inputRowMetaXml = inputRowMetaXml;
  }

  @Override public PCollection<KettleRow> expand( PCollection<KettleRow> input ) {

    try {

      // Only initialize once on this node/vm
      //
      BeamKettle.init();

      // Create a new step function, initializes the step
      //
      StepFn stepFn = new StepFn( variableValues, stepname, stepPluginId, stepMetaInterfaceXml, inputRowMetaXml);

      PCollection<KettleRow> output = input.apply( ParDo.of( stepFn ) );

      return output;

    } catch ( Exception e ) {
      numErrors.inc();
      LOG.error("Error transforming data in step '"+ stepname +"'", e);
      throw new RuntimeException( "Error transforming data in step", e );
    }

  }

}
