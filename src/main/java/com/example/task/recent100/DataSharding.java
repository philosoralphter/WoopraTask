package com.example.task.recent100;

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.Charset;

public class DataSharding {
  public int dataSharding(String path, String dir, int recordNumPerFile) {
    int fileCounter = 0;
    try {
      File file = new File(path);
      BufferedReader br = new BufferedReader(new FileReader(file));
      String line = br.readLine();
      int lineCounter = 0;
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
    } catch (FileNotFoundException e1) {
      // TODO Auto-generated catch block
      e1.printStackTrace();
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    return fileCounter;
  }
}
