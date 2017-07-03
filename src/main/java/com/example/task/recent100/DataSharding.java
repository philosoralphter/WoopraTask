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
 * DataSharding
 *
 * @author Sha Wang
 * @version 0.1 2017-6-29
 */
public class DataSharding {
  public int dataSharding(String path, String dir, int recordNumPerFile) throws IOException {
    int fileCounter = 0;
    File file = new File(path);
    BufferedReader br = new BufferedReader(new FileReader(file));
    String line = br.readLine();
    int lineCounter = 0;
    //read all lines, than output to small file
    while (line != null) {
      //create one small file
      File newFile = new File(dir + file.getName() + "_" + String.format("%05d", fileCounter++));
      OutputStream out = new BufferedOutputStream(new FileOutputStream(newFile));
      while (line != null && lineCounter < recordNumPerFile) {
        byte[] bytes = (line + System.lineSeparator()).getBytes(Charset.defaultCharset());
        out.write(bytes);
        line = br.readLine();
        lineCounter++;
      }
      out.close();
      lineCounter = 0;
    }

    return fileCounter;
  }
}
