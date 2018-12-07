package org.kettle.beam.core.fn;

import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.commons.lang.StringUtils;
import org.kettle.beam.core.BeamKettle;
import org.kettle.beam.core.KettleRow;
import org.kettle.beam.core.shared.VariableValue;
import org.kettle.beam.core.util.KettleBeamUtil;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleStepException;
import org.pentaho.di.core.logging.LogLevel;
import org.pentaho.di.core.plugins.PluginRegistry;
import org.pentaho.di.core.plugins.StepPluginType;
import org.pentaho.di.core.row.RowMeta;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.xml.XMLHandler;
import org.pentaho.di.trans.RowProducer;
import org.pentaho.di.trans.SingleThreadedTransExecutor;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransHopMeta;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.RowAdapter;
import org.pentaho.di.trans.step.RowListener;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaDataCombi;
import org.pentaho.di.trans.step.StepMetaInterface;
import org.pentaho.di.trans.steps.dummytrans.DummyTransMeta;
import org.pentaho.di.trans.steps.injector.InjectorMeta;
import org.pentaho.metastore.api.IMetaStore;
import org.pentaho.metastore.stores.memory.MemoryMetaStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class StepFn extends DoFn<KettleRow, KettleRow> {

  public static final String INJECTOR_STEP_NAME = "_INJECTOR_";

  protected List<VariableValue> variableValues;
  protected String stepname;
  protected String stepPluginId;
  protected String stepMetaInterfaceXml;
  protected String rowMetaXml;
  protected List<String> targetSteps;

  // Log and count parse errors.
  private static final Logger LOG = LoggerFactory.getLogger( StepFn.class );
  private final Counter numErrors = Metrics.counter( "main", "StepProcessErrors" );

  private transient TransMeta transMeta = null;
  private transient StepMeta stepMeta = null;
  private transient RowMetaInterface inputRowMeta;
  private transient RowMetaInterface outputRowMeta;
  private transient List<StepMetaDataCombi> stepCombis;
  private transient Trans trans;
  private transient RowProducer rowProducer;
  private transient RowListener rowListener;
  private transient List<Object[]> resultRows;
  private transient List<List<Object[]>> targetResultRowsList;
  private transient List<RowMetaInterface> targetRowMetas;

  private transient TupleTag<KettleRow> mainTupleTag;
  private transient List<TupleTag<KettleRow>> tupleTagList;

  private transient Counter initCounter;
  private transient Counter readCounter;
  private transient Counter writtenCounter;

  private transient SingleThreadedTransExecutor executor;

  public StepFn() {
    // Don't expect this to be called.
    //
    resultRows = new ArrayList<>();
    variableValues = new ArrayList<>();
  }

  public StepFn( List<VariableValue> variableValues, String stepname, String stepPluginId, String stepMetaInterfaceXml, String inputRowMetaXml, List<String> targetSteps )
    throws KettleException, IOException {
    this();
    this.variableValues = variableValues;
    this.stepname = stepname;
    this.stepPluginId = stepPluginId;
    this.stepMetaInterfaceXml = stepMetaInterfaceXml;
    this.rowMetaXml = inputRowMetaXml;
    this.targetSteps = targetSteps;
  }

  @ProcessElement
  public void processElement( ProcessContext context ) {

    try {

      if ( transMeta == null ) {

        // Just to make sure
        BeamKettle.init();

        // TODO: pass in real metastore objects through JSON serialization
        //
        IMetaStore metaStore = new MemoryMetaStore();

        // Create a very simple new transformation to run single threaded...
        // Single threaded...
        //
        transMeta = new TransMeta();
        transMeta.setMetaStore( metaStore );

        // Give steps variables from above
        //
        for ( VariableValue variableValue : variableValues ) {
          if ( StringUtils.isNotEmpty( variableValue.getVariable() ) ) {
            transMeta.setVariable( variableValue.getVariable(), variableValue.getValue() );
          }
        }

        // Input row metadata...
        //
        inputRowMeta = new RowMeta( XMLHandler.getSubNode( XMLHandler.loadXMLString( rowMetaXml ), RowMeta.XML_META_TAG ) );

        // Create an Injector step with the right row layout...
        // This will help all steps see the row layout statically...
        //
        InjectorMeta injectorMeta = new InjectorMeta();
        injectorMeta.allocate( inputRowMeta.size() );
        for ( int i = 0; i < inputRowMeta.size(); i++ ) {
          ValueMetaInterface valueMeta = inputRowMeta.getValueMeta( i );
          injectorMeta.getFieldname()[ i ] = valueMeta.getName();
          injectorMeta.getType()[ i ] = valueMeta.getType();
          injectorMeta.getLength()[ i ] = valueMeta.getLength();
          injectorMeta.getPrecision()[ i ] = valueMeta.getPrecision();
        }
        StepMeta injectorStepMeta = new StepMeta( INJECTOR_STEP_NAME, injectorMeta );
        injectorStepMeta.setLocation( 200, 200 );
        injectorStepMeta.setDraw( true );
        transMeta.addStep( injectorStepMeta );

        // Our main step writes to a bunch of targets
        // Add a dummy step for each one so the step can target them
        //
        int targetLocationY = 200;
        for ( String targetStep : targetSteps ) {
          DummyTransMeta dummyMeta = new DummyTransMeta();
          StepMeta dummyStepMeta = new StepMeta( targetStep, dummyMeta );
          dummyStepMeta.setLocation( 600, targetLocationY );
          dummyStepMeta.setDraw( true );
          targetLocationY += 150;

          transMeta.addStep( dummyStepMeta );
        }

        stepCombis = new ArrayList<>(  );

        // The main step inflated from XML metadata...
        //
        PluginRegistry registry = PluginRegistry.getInstance();
        StepMetaInterface stepMetaInterface = registry.loadClass( StepPluginType.class, stepPluginId, StepMetaInterface.class );
        stepMetaInterface.loadXML( XMLHandler.getSubNode( XMLHandler.loadXMLString( stepMetaInterfaceXml ), StepMeta.XML_TAG ), new ArrayList<>(), new MemoryMetaStore() );
        stepMeta = new StepMeta( stepname, stepMetaInterface );
        stepMeta.setStepID( stepPluginId );
        stepMeta.setLocation( 400, 200 );
        stepMeta.setDraw( true );
        transMeta.addStep( stepMeta );
        transMeta.addTransHop( new TransHopMeta( injectorStepMeta, stepMeta ) );

        // The target hops as well
        //
        for ( String targetStep : targetSteps ) {
          transMeta.addTransHop( new TransHopMeta( stepMeta, transMeta.findStep( targetStep ) ) );
        }
        stepMetaInterface.searchInfoAndTargetSteps( transMeta.getSteps() );

        FileOutputStream fos = new FileOutputStream( "/tmp/" + stepname + ".ktr" );
        fos.write( transMeta.getXML().getBytes() );
        fos.close();

        // This one is single threaded folks
        //
        transMeta.setTransformationType( TransMeta.TransformationType.SingleThreaded );

        // Create the transformation...
        //
        trans = new Trans( transMeta );
        trans.setLogLevel( LogLevel.ERROR );
        trans.setMetaStore( metaStore );
        trans.prepareExecution( null );

        rowProducer = trans.addRowProducer( INJECTOR_STEP_NAME, 0 );

        // Find the right interfaces for execution later...
        //
        StepMetaDataCombi injectorCombi = findCombi(trans, INJECTOR_STEP_NAME);
        stepCombis.add(injectorCombi);

        StepMetaDataCombi stepCombi = findCombi( trans, stepname );
        stepCombis.add(stepCombi);
        outputRowMeta = transMeta.getStepFields( stepname );

        if (targetSteps.isEmpty()) {
          rowListener = new RowAdapter() {
            @Override public void rowWrittenEvent( RowMetaInterface rowMeta, Object[] row ) throws KettleStepException {

              resultRows.add( row );
            }
          };
          stepCombi.step.addRowListener( rowListener );
        }

        // Create a list of TupleTag to direct the target rows
        //
        mainTupleTag = new TupleTag<KettleRow>( stepname ) {
        };
        tupleTagList = new ArrayList<>();

        // The lists in here will contain all the rows that ended up in the various target steps (if any)
        //
        targetRowMetas = new ArrayList<>(  );
        targetResultRowsList = new ArrayList<>();

        for (String targetStep : targetSteps) {
          StepMetaDataCombi targetCombi = findCombi(trans, targetStep);
          stepCombis.add(targetCombi);
          targetRowMetas.add(transMeta.getStepFields( stepCombi.stepname ));

          String tupleId = KettleBeamUtil.createTargetTupleId( stepname, targetStep );
          TupleTag<KettleRow> tupleTag = new TupleTag<KettleRow>( tupleId ) {
          };
          tupleTagList.add( tupleTag );
          final List<Object[]> targetResultRows = new ArrayList<>();
          targetResultRowsList.add( targetResultRows );

          targetCombi.step.addRowListener( new RowAdapter() {
            @Override public void rowReadEvent( RowMetaInterface rowMeta, Object[] row ) throws KettleStepException {
              // We send the target row to a specific list...
              //
              targetResultRows.add( row );
            }
          } );
        }

        executor = new SingleThreadedTransExecutor( trans );

        // Initialize the steps...
        //
        executor.init();

        initCounter = Metrics.counter( "init", stepname );
        readCounter = Metrics.counter( "read", stepname );
        writtenCounter = Metrics.counter( "written", stepname );

        initCounter.inc();

        // Doesn't really start the threads in single threaded mode
        // Just sets some flags all over the place
        //
        trans.startThreads();

        resultRows = new ArrayList<>();
      }

      ///////////////////////////////////////////
      //
      // Above is all initialisation
      //
      ///////////////////////////////////////////

      KettleRow inputRow = KettleBeamUtil.copyKettleRow( context.element(), inputRowMeta);

      // Empty all the row buffers for another iteration
      //
      resultRows.clear();
      for ( int t = 0; t < targetSteps.size(); t++ ) {
        targetResultRowsList.get( t ).clear();
      }

      // Get one row, pass it through the given stepInterface copy
      // Retrieve the rows and pass them to the processContext
      //
      // KettleRow inputRow = processContext.element();

      // Pass the row to the input rowset
      //
      rowProducer.putRow( inputRowMeta, inputRow.getRow() );
      readCounter.inc();

      // Execute all steps in the transformation
      //
      executor.oneIteration();

      // Evaluate the results...
      //

      // Pass all rows in the output to the process context
      //
      // System.out.println("Rows read in main output of step '"+stepname+"' : "+resultRows.size());
      for ( Object[] resultRow : resultRows ) {

        // Pass the row to the process context
        //
        context.output( mainTupleTag, new KettleRow( resultRow ) );
        writtenCounter.inc();
      }

      // Pass whatever ended up on the target nodes
      //
      for ( int t = 0; t < targetResultRowsList.size(); t++ ) {
        List<Object[]> targetRowsList = targetResultRowsList.get( t );
        TupleTag<KettleRow> tupleTag = tupleTagList.get( t );

        for ( Object[] targetRow : targetRowsList ) {
          context.output( tupleTag, new KettleRow( targetRow ) );
        }
      }

    } catch ( Exception e ) {
      numErrors.inc();
      LOG.info( "Step execution error :" + e.getMessage() );
      throw new RuntimeException( "Error executing StepFn", e );
    }
  }

  private StepMetaDataCombi findCombi( Trans trans, String stepname ) {
    for (StepMetaDataCombi combi : trans.getSteps()) {
      if (combi.stepname.equals( stepname )) {
        return combi;
      }
    }
    throw new RuntimeException( "Configuration error, step '"+stepname+"' not found in transformation" );
  }


}
