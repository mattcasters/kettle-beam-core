package org.kettle.beam.core.transform;

import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.apache.commons.lang.StringUtils;
import org.kettle.beam.core.BeamKettle;
import org.kettle.beam.core.KettleRow;
import org.kettle.beam.core.fn.KettleToStringFn;
import org.pentaho.di.core.row.RowMeta;
import org.pentaho.di.core.xml.XMLHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

public class BeamOutputTransform extends PTransform<PCollection<KettleRow>, PDone> {

  // These non-transient privates get serialized to spread across nodes
  //
  private String stepname;
  private String outputLocation;
  private String filePrefix;
  private String fileSuffix;
  private String separator;
  private String enclosure;
  private String rowMetaXml;

  // Log and count errors.
  private static final Logger LOG = LoggerFactory.getLogger( BeamOutputTransform.class );
  private static final Counter numErrors = Metrics.counter( "main", "BeamOutputError" );

  public BeamOutputTransform() {
  }

  public BeamOutputTransform( String stepname, String outputLocation, String filePrefix, String fileSuffix, String separator, String enclosure, String rowMetaXml ) {
    this.stepname = stepname;
    this.outputLocation = outputLocation;
    this.filePrefix = filePrefix;
    this.fileSuffix = fileSuffix;
    this.separator = separator;
    this.enclosure = enclosure;
    this.rowMetaXml = rowMetaXml;
  }

  @Override public PDone expand( PCollection<KettleRow> input ) {

    try {
      // Only initialize once on this node/vm
      //
      BeamKettle.init();

      // Inflate the metadata on the node where this is running...
      //
      RowMeta rowMeta = new RowMeta( XMLHandler.getSubNode( XMLHandler.loadXMLString( rowMetaXml ), RowMeta.XML_META_TAG ) );

      // This is the end of a computing chain, we write out the results
      //


      // We read a bunch of Strings, one per line basically
      //
      PCollection<String> stringCollection = input.apply( stepname, ParDo.of( new KettleToStringFn( outputLocation, separator, enclosure, rowMetaXml ) ) );

      // We need to transform these lines into a file and then we're PDone
      //
      TextIO.Write write = TextIO.write();
      if ( StringUtils.isNotEmpty(outputLocation)) {
        String outputPrefix = outputLocation;
        if (!outputPrefix.endsWith( File.separator)) {
          outputPrefix+=File.separator;
        }
        if (StringUtils.isNotEmpty( filePrefix )) {
          outputPrefix+=filePrefix;
        }
        write = write.to( outputPrefix );
      }
      if (StringUtils.isNotEmpty( fileSuffix )) {
        write = write.withSuffix( fileSuffix );
      }
      stringCollection.apply(write);

      // Get it over with
      //
      return PDone.in(input.getPipeline());

    } catch ( Exception e ) {
      numErrors.inc();
      LOG.error( "Error in beam output transform", e );
      throw new RuntimeException( "Error in beam output transform", e );
    }
  }


  /**
   * Gets stepname
   *
   * @return value of stepname
   */
  public String getStepname() {
    return stepname;
  }

  /**
   * @param stepname The stepname to set
   */
  public void setStepname( String stepname ) {
    this.stepname = stepname;
  }

  /**
   * Gets outputLocation
   *
   * @return value of outputLocation
   */
  public String getOutputLocation() {
    return outputLocation;
  }

  /**
   * @param outputLocation The outputLocation to set
   */
  public void setOutputLocation( String outputLocation ) {
    this.outputLocation = outputLocation;
  }

  /**
   * Gets filePrefix
   *
   * @return value of filePrefix
   */
  public String getFilePrefix() {
    return filePrefix;
  }

  /**
   * @param filePrefix The filePrefix to set
   */
  public void setFilePrefix( String filePrefix ) {
    this.filePrefix = filePrefix;
  }

  /**
   * Gets fileSuffix
   *
   * @return value of fileSuffix
   */
  public String getFileSuffix() {
    return fileSuffix;
  }

  /**
   * @param fileSuffix The fileSuffix to set
   */
  public void setFileSuffix( String fileSuffix ) {
    this.fileSuffix = fileSuffix;
  }

  /**
   * Gets separator
   *
   * @return value of separator
   */
  public String getSeparator() {
    return separator;
  }

  /**
   * @param separator The separator to set
   */
  public void setSeparator( String separator ) {
    this.separator = separator;
  }

  /**
   * Gets enclosure
   *
   * @return value of enclosure
   */
  public String getEnclosure() {
    return enclosure;
  }

  /**
   * @param enclosure The enclosure to set
   */
  public void setEnclosure( String enclosure ) {
    this.enclosure = enclosure;
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
}
