package com.example.task.recent100;

public class Constant {
  public static final String JOB_REQUEST = "1";
  public static final String JOB_FINISHED = "0";
  public static final String NO_MORE_JOB = "-1";
  public static final int RECENT_N_COUNT = 100;
  //public static final int RECENT_N_COUNT = 2;
  public static final String CREATE_MAPPER_PROCESS = "java com.example.task.recent100.Mapper";
  public static final String CREATE_REDUCER_PROCESS = "java com.example.task.recent100.Reducer";
  public static final int MAPPER_COUNT = 4;
  public static final int REDUCER_COUNT = 1;
  public static final int RECORD_NUM_PER_FILE = 200;
  //public static final int RECORD_NUM_PER_FILE = 3;
  public static final String DEFAULT_ADDRESS = "/Users/shawang/Documents/workspace/woopra/file.txt";
  public static final int BACK_LOG = 100;
  public static final String MAPPER = "_Mapper_";
  public static final String REDUCER = "_Reducer_";
  public static final String REPORT = "_Report";
  public static final String REDUCER_DONE = "_Reducer_Done";
}
