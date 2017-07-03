package com.example.task.recent100;

/**
 * Constant: all constant used in project.
 *
 * @author Sha Wang
 * @version 0.1 2017-6-29
 */
public class Constant {
  //Mapper and Master communicate
  public static final String JOB_REQUEST = "1";
  public static final String JOB_FINISHED = "0";
  public static final String NO_MORE_JOB = "-1";

  //Block number for master socket
  public static final int BACK_LOG = 100;

  //Default value for Recent_N_Count, used for num of records in result
  public static final int RECENT_N_COUNT = 100;

  //start mapper, reducer process
  public static final String CREATE_MAPPER_PROCESS = "java com.example.task.recent100.Mapper";
  public static final String CREATE_REDUCER_PROCESS = "java com.example.task.recent100.Reducer";

  //Default value for Mapper_Count
  public static final int MAPPER_COUNT = 4;
  public static final int REDUCER_COUNT = 1;

  //Default value for Record_Num_Per_File, used for data sharding
  public static final int RECORD_NUM_PER_FILE = 200;

  //filename
  public static final String MAPPER = "_Mapper_";
  public static final String REDUCER = "_Reducer_";
  public static final String REPORT = "_report.csv";
  public static final String REDUCER_DONE = "_Reducer_Done";
  public static final String DEDUP = "_dedup";
}
