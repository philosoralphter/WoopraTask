package com.example.task.recent100;

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.Charset;

public class Collector {
  public String generateReport(String path, String tmpPath, int reduceCount) throws IOException {
    File outFile = new File(path + Constant.REPORT);
    OutputStream out = new BufferedOutputStream(new FileOutputStream(outFile));

    //output each Reducer result to final report
    for (int i = 0; i < reduceCount; i++) {
      BufferedReader br =
          new BufferedReader(new FileReader(new File(tmpPath + Constant.REDUCER + i)));
      String line;
      while ((line = br.readLine()) != null) {
        byte[] bytes = (line + System.lineSeparator()).getBytes(Charset.defaultCharset());
        out.write(bytes);
      }
    }
    out.close();
    return outFile.getPath();
  }
}
