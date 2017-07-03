package com.example.task.recent100;

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.Charset;

/**
 * Reducer
 *
 * @author Sha Wang
 * @version 0.1 2017-7-1
 */
public class Reducer {
  private MapHeap heap;
  private File file;
  private OutputStream out;

  public static void main(String[] args) throws IOException {
    //parser the arguments
    int reducerId = Integer.valueOf(args[0]);
    String path = args[1];
    int mapperCount = Integer.valueOf(args[2]);
    int recentNCount = Integer.valueOf(args[3]);

    //start the real job
    Reducer reducer = new Reducer();
    reducer.run(path, mapperCount, reducerId, recentNCount);
  }

  private void run(String path, int mapperCount, int reducerId, int recentNCount)
      throws IOException {
    //initialize
    setup(path, reducerId, recentNCount);

    //read data from each Mapper output and reduce
    for (int i = 0; i < mapperCount; i++) {
      File file = new File(path + Constant.MAPPER + i);
      BufferedReader br = new BufferedReader(new FileReader(file));
      String line;
      while ((line = br.readLine()) != null) {
        int sepIndex = line.indexOf(',');
        reduce(line.substring(0, sepIndex), line.substring(sepIndex + 1));
      }
    }

    //final
    cleanup(path);
  }

  //initialize, new heap and open output stream
  private void setup(String path, int reducerId, int recentNCount) throws IOException {
    this.heap = new MapHeap(recentNCount);
    this.file = new File(path + Constant.REDUCER + reducerId);
    this.out = new BufferedOutputStream(new FileOutputStream(this.file));
  }

  //clean output result to file. Called once at the end of the task.
  private void cleanup(String path) throws IOException {
    //generate result from heap
    UserRecord[] result = new UserRecord[heap.size()];
    int recentIndex = 0;
    while (heap.getPeek() != null) {
      result[recentIndex++] = heap.pop();
    }

    //output
    for (int i = 1; i <= result.length; i++) {
      UserRecord tmp = result[result.length - i];
      output(i, tmp.getPid() + "," + tmp.getTime());
    }
    this.out.close();

    //generate reducer done file
    this.file = new File(path + Constant.REDUCER_DONE);
    this.out = new BufferedOutputStream(new FileOutputStream(this.file));
    this.out.close();
  }

  //One piece of data processing
  private void reduce(String key, String value) {
    heap.put(new UserRecord(key, Long.valueOf(value)));
  }

  //output result to file
  private void output(int key, String value) throws IOException {
    byte[] bytes = (key + "," + value + System.lineSeparator()).getBytes(Charset.defaultCharset());
    this.out.write(bytes);
  }
}
