package org.kettle.beam.core.util;

import org.kettle.beam.core.KettleRow;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.row.RowDataUtil;
import org.pentaho.di.core.row.RowMetaInterface;

public class KettleBeamUtil {

  public static final String createTargetTupleId(String stepname, String targetStepname){
    return stepname+" - TARGET - "+targetStepname;
  }

  public static final KettleRow copyKettleRow( KettleRow kettleRow, RowMetaInterface rowMeta ) throws KettleException {
    Object[] newRow = RowDataUtil.createResizedCopy(kettleRow.getRow(), rowMeta.size());
    return new KettleRow(newRow);
  }
 }
