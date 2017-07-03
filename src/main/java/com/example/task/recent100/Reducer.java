package com.example.task.recent100;

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.Charset;

public class Reducer {
  private MapHeap heap;
  private File file;
  private OutputStream out;

  public static void main(String[] args) throws IOException {

    Reducer reducer = new Reducer();
    String path = args[1];
    int mapperCount = Integer.valueOf(args[2]);
    int reducerId = Integer.valueOf(args[0]);
    reducer.run(path, mapperCount, reducerId);
  }

  private void run(String path, int mapperCount, int reducerId) throws IOException {

    setup(path, reducerId);
    //read data from each Mapper output
    for (int i = 0; i < mapperCount; i++) {
      File file = new File(path + Constant.MAPPER + i);
      BufferedReader br = new BufferedReader(new FileReader(file));
      String line;
      while ((line = br.readLine()) != null) {
        int sepIndex = line.indexOf(',');
        reduce(line.substring(0, sepIndex), line.substring(sepIndex + 1));
      }
    }
    cleanup();
    this.out.close();

    this.file = new File(path + Constant.REDUCER_DONE);
    this.out = new BufferedOutputStream(new FileOutputStream(this.file));
    this.out.close();
  }

  //initialize
  private void setup(String path, int reducerId) throws IOException {
    this.heap = new MapHeap(Constant.RECENT_N_COUNT);
    this.file = new File(path + Constant.REDUCER + reducerId);
    this.out = new BufferedOutputStream(new FileOutputStream(this.file));
  }

  //clear output result to file
  private void cleanup() throws IOException {
    UserRecord[] result = new UserRecord[heap.size()];
    int recentIndex = 0;
    while (heap.getPeek() != null) {
      result[recentIndex++] = heap.pop();
    }
    for (int i = 1; i <= result.length; i++) {
      UserRecord tmp = result[result.length - i];
      output(i, tmp.getPid() + "," + tmp.getTime());
    }
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
