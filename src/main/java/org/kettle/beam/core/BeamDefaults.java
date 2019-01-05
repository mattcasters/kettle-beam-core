package org.kettle.beam.core;

public class BeamDefaults {
  public static final String STRING_BEAM_INPUT_PLUGIN_ID = "BeamInput";
  public static final String STRING_BEAM_OUTPUT_PLUGIN_ID = "BeamOutput";

  public static final String STRING_KETTLE_BEAM = "Kettle Beam";

  public static final String PUBSUB_MESSAGE_TYPE_AVROS    = "Avros";
  public static final String PUBSUB_MESSAGE_TYPE_PROTOBUF = "protobuf";
  public static final String PUBSUB_MESSAGE_TYPE_STRING   = "String";
  public static final String PUBSUB_MESSAGE_TYPE_MESSAGE  = "PubsubMessage";

  public static final String[] PUBSUB_MESSAGE_TYPES = new String[] {
    // PUBSUB_MESSAGE_TYPE_AVROS,
    // PUBSUB_MESSAGE_TYPE_PROTOBUF,
    PUBSUB_MESSAGE_TYPE_STRING,
    PUBSUB_MESSAGE_TYPE_MESSAGE,
  };

}
