package org.kettle.beam.core.shared;

import org.pentaho.di.core.exception.KettleException;

public enum AggregationType {
  SUM, COUNT, MIN, MAX,
  ;

  public static final AggregationType getTypeFromName(String name) throws KettleException {
    for (AggregationType type : values()) {
      if (name.equals( type.name() )) {
        return type;
      }
    }
    throw new KettleException( "Aggregation type '"+name+"' is not recognized or supported" );
  }
}
