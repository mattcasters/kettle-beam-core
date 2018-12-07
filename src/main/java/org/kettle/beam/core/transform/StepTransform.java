package org.kettle.beam.core.transform;

import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.kettle.beam.core.BeamKettle;
import org.kettle.beam.core.KettleRow;
import org.kettle.beam.core.fn.StepFn;
import org.kettle.beam.core.shared.VariableValue;
import org.kettle.beam.core.util.KettleBeamUtil;
import org.pentaho.di.core.exception.KettleException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class StepTransform extends PTransform<PCollection<KettleRow>, PCollectionTuple> {

  protected List<VariableValue> variableValues;
  protected String metastoreJson;
  protected List<String> stepPluginClasses;
  protected List<String> xpPluginClasses;
  protected String stepname;
  protected String stepPluginId;
  protected String inputRowMetaXml;
  protected String stepMetaInterfaceXml;
  protected List<String> targetSteps;

  // Log and count errors.
  private static final Logger LOG = LoggerFactory.getLogger( StepTransform.class );
  private static final Counter numErrors = Metrics.counter( "main", "StepErrors" );

  public StepTransform() {
    variableValues = new ArrayList<>();
  }

  public StepTransform( List<VariableValue> variableValues, String metastoreJson, List<String> stepPluginClasses, List<String> xpPluginClasses, String stepname, String stepPluginId, String stepMetaInterfaceXml, String inputRowMetaXml, List<String> targetSteps )
    throws KettleException, IOException {
    this.variableValues = variableValues;
    this.metastoreJson = metastoreJson;
    this.stepPluginClasses = stepPluginClasses;
    this.xpPluginClasses = xpPluginClasses;
    this.stepname = stepname;
    this.stepPluginId = stepPluginId;
    this.stepMetaInterfaceXml = stepMetaInterfaceXml;
    this.inputRowMetaXml = inputRowMetaXml;
    this.targetSteps = targetSteps;
  }

  @Override public PCollectionTuple expand( PCollection<KettleRow> input ) {
    try {

      // Only initialize once on this node/vm
      //
      BeamKettle.init(stepPluginClasses, xpPluginClasses);

      // Create a TupleTag list
      //
      TupleTag<KettleRow> mainTupleTag = new TupleTag<KettleRow>( stepname ) {
      };
      List<TupleTag<KettleRow>> tupleTags = new ArrayList<>();
      TupleTagList tupleTagList = null;
      for ( String targetStep : targetSteps ) {
        String tupleId = KettleBeamUtil.createTargetTupleId( stepname, targetStep );
        TupleTag<KettleRow> tupleTag = new TupleTag<KettleRow>( tupleId ) {
        };
        tupleTags.add( tupleTag );
        if ( tupleTagList == null ) {
          tupleTagList = TupleTagList.of( tupleTag );
        } else {
          tupleTagList = tupleTagList.and( tupleTag );
        }
      }
      if ( tupleTagList == null ) {
        tupleTagList = TupleTagList.empty();
      }

      // Create a new step function, initializes the step
      //
      StepFn stepFn = new StepFn( variableValues, metastoreJson, stepPluginClasses, xpPluginClasses, stepname, stepPluginId, stepMetaInterfaceXml, inputRowMetaXml, targetSteps );

      PCollectionTuple collectionTuple = input.apply(
        ParDo
          .of( stepFn )
          .withOutputTags( mainTupleTag, tupleTagList ) )
        ;

      // In the tuple is everything we need to find.
      // Just make sure to retrieve the PCollections using the correct Tuple ID
      // Use KettleBeamUtil.createTargetTupleId() to make sure
      //
      return collectionTuple;
    } catch ( Exception e ) {
      numErrors.inc();
      LOG.error( "Error transforming data in step '" + stepname + "'", e );
      throw new RuntimeException( "Error transforming data in step", e );
    }

  }

}
