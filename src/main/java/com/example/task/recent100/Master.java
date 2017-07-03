package com.example.task.recent100;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Date;
import java.util.Locale;
import java.util.Scanner;
import java.util.concurrent.TimeUnit;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

public class Master {

  public static void main(String[] args) throws Exception {
    //commandLine parser
    Options options = new Options();
    options.addOption("h", false, "help information");
    Option f =
        Option.builder("f")
            .required(false)
            .hasArg()
            .argName("filename")
            .desc("The file is a line separated JSON, containing records of visitor sessions.")
            .build();
    options.addOption(f);
    Option m =
        Option.builder("m")
            .required(false)
            .type(Number.class)
            .hasArg()
            .argName("mapperCount")
            .desc("The count of Mapper.")
            .build();
    options.addOption(m);
    Option s =
        Option.builder("s")
            .required(false)
            .type(Number.class)
            .hasArg()
            .argName("size")
            .desc("The number of records per shard.")
            .build();
    options.addOption(s);

    //parser
    CommandLineParser parser = new DefaultParser();
    CommandLine cmd = parser.parse(options, args);

    if (cmd.hasOption('h')) {
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp("Master", options);
      return;
    }

    String path;
    int mapperCount = Constant.MAPPER_COUNT;
    int recordNumPerFile = Constant.RECORD_NUM_PER_FILE;
    try {
      path = cmd.getOptionValue("f");
      if (path == null || path.isEmpty()) {
        throw new ParseException("filename");
      }

      if (cmd.hasOption('m')) {
        mapperCount = ((Number) cmd.getParsedOptionValue("m")).intValue();
        if (mapperCount <= 0) {
          throw new ParseException("mapper count");
        }
      }

      if (cmd.hasOption('s')) {
        recordNumPerFile = ((Number) cmd.getParsedOptionValue("s")).intValue();
        if (recordNumPerFile <= 0) {
          throw new ParseException("records per shard");
        }
      }

    } catch (ParseException e) {
      System.out.println("Ivalid arguments: " + e.getMessage());
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp("Master", options);
      return;
    }

    System.out.println("MapReduce arguments:");
    System.out.println("  Input: " + path);
    System.out.println("  #(Records) Per Shard: " + String.valueOf(recordNumPerFile));
    System.out.println("  #(Mapper): " + String.valueOf(mapperCount));
    System.out.println("  #(Reducer): 1 (non-configurable)");

    Master master = new Master();

    /**
     * There should sharding the data before Master. Only for the task, we sharding data in the
     * Master.
     */

    // Sharding the data.
    File infile = new File(path);
    String tmpPath = infile.getAbsolutePath() + "_tmp_" + (new Date()).getTime();
    File dir = new File(tmpPath);
    dir.mkdirs();
    tmpPath = dir.getPath() + "/";
    int fileCount = master.fileSharding(path, tmpPath, recordNumPerFile);
    tmpPath = dir.getPath() + "/" + infile.getName();

    int fileIndex = 0;
    int finishedMapper = 0;

    // Start network monitoring
    InetAddress addr = InetAddress.getLocalHost();
    ServerSocket serverSocket = new ServerSocket(0, Constant.BACK_LOG, addr);
    String ip = addr.getHostAddress();
    int port = serverSocket.getLocalPort();
    System.out.println("Started server: " + ip + ":" + port);

    // start Mapper
    master.startMapper(ip, port, tmpPath, mapperCount);
    System.out.println("Started " + mapperCount + " Mapper.");

    // listen from Mappers, until all job done.
    boolean mapperFinished = false;
    while (!mapperFinished) {
      // a "blocking" call which waits until a connection is requested
      Socket clientSocket = serverSocket.accept();

      // open up IO streams
      Scanner in = new Scanner(new BufferedInputStream(clientSocket.getInputStream()), "UTF-8");
      in.useLocale(Locale.US);
      PrintWriter out = new PrintWriter(clientSocket.getOutputStream(), true);

      // waits for data and reads it in until connection dies
      // readLine() blocks until the server receives a new line from
      // client
      String cMsg = in.nextLine();
      // assign job
      if (cMsg.equals(Constant.JOB_REQUEST)) {
        if (fileIndex < fileCount) {
          out.println(fileIndex);
          fileIndex++;
        } else {
          out.println(Constant.NO_MORE_JOB);
        }
      } else {
        out.println(Constant.NO_MORE_JOB);
        if (++finishedMapper == mapperCount) {
          mapperFinished = true;
        }
      }
      // close IO streams, then socket
      out.close();
      in.close();
      clientSocket.close();
    }
    System.out.println("Mappers finished!");

    /**
     * Before reducer, there should shuffle the data after Mapper process. Then distribute the
     * sorted data to different Reducer. After Reducer handle, Collector the result from the
     * different Reducer.
     *
     * <p>For this task, we don't need shuffle the data, we only need one Reducer to process the
     * data.
     */

    // start Reducer
    master.startReducer(tmpPath, mapperCount);
    System.out.println("Started 1 Reducer.");

    // Use done files to confirm Reducer has finished.
    while (true) {
      File done_file = new File(tmpPath + Constant.REDUCER_DONE);
      if (done_file.exists()) {
        System.out.println("Reducers finished!");
        break;
      }
      //TimeUnit.MINUTES.sleep(1);
      TimeUnit.SECONDS.sleep(5);
    }
    System.out.println("Reducer finished!");

    // do Collector(generate the report)
    String resultPath = master.doCollector(path, tmpPath);
    System.out.println("All DONE!\nReport is: " + resultPath);
  }

  // data sharding
  public int fileSharding(String path, String dir, int recordNumPerFile) {
    DataSharding ds = new DataSharding();
    return ds.dataSharding(path, dir, recordNumPerFile);
  }

  // start Mapper
  public void startMapper(String ip, int port, String path, int mapperCount) throws IOException {
    Runtime run = Runtime.getRuntime();
    for (int mapperIndex = 0; mapperIndex < mapperCount; mapperIndex++) {
      Process mapper =
          run.exec(
              Constant.CREATE_MAPPER_PROCESS
                  + " "
                  + mapperIndex
                  + " "
                  + ip
                  + " "
                  + port
                  + " "
                  + path);
    }
  }

  // start Reducer
  public void startReducer(String path, int mapperCount) throws IOException {
    Runtime run = Runtime.getRuntime();
    for (int reducerIndex = 0; reducerIndex < Constant.REDUCER_COUNT; reducerIndex++) {
      Process reducer =
          run.exec(
              Constant.CREATE_REDUCER_PROCESS
                  + " "
                  + reducerIndex
                  + " "
                  + path
                  + " "
                  + mapperCount);
    }
  }

  // do Collector(generate the report)
  public String doCollector(String path, String tmpPath) throws IOException {
    Collector collector = new Collector();
    return collector.generateReport(path, tmpPath, Constant.REDUCER_COUNT);
  }
}
