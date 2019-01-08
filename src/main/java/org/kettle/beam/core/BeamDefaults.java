package org.kettle.beam.core;

public class BeamDefaults {
  public static final String STRING_BEAM_INPUT_PLUGIN_ID = "BeamInput";
  public static final String STRING_BEAM_OUTPUT_PLUGIN_ID = "BeamOutput";

  public static final String STRING_BEAM_SUBSCRIBE_PLUGIN_ID = "BeamSubscribe";
  public static final String STRING_BEAM_PUBLISH_PLUGIN_ID = "BeamPublish";

  public static final String STRING_MEMORY_GROUP_BY_PLUGIN_ID = "MemoryGroupBy";
  public static final String STRING_MERGE_JOIN_PLUGIN_ID = "MergeJoin";

  public static final String STRING_BEAM_WINDOW_PLUGIN_ID = "BeamWindow";

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


  public static final String WINDOW_TYPE_FIXED    = "Fixed";
  public static final String WINDOW_TYPE_SLIDING = "Sliding";
  public static final String WINDOW_TYPE_SESSION = "Session";
  public static final String WINDOW_TYPE_GLOBAL = "Global";

  public static final String[] WINDOW_TYPES = new String[] {
    WINDOW_TYPE_FIXED,
    WINDOW_TYPE_SLIDING,
    WINDOW_TYPE_SESSION,
    WINDOW_TYPE_GLOBAL
  };

}
