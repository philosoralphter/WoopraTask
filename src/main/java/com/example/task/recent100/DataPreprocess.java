package com.example.task.recent100;

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Map;
import java.util.Stack;

import org.json.JSONObject;

/**
 * DataPreprocess
 *
 * @author Sha Wang
 * @version 0.1 2017-6-29
 */
public class DataPreprocess {
  public static void main(String[] args) throws IOException {
    //first generate pid mapping, then create new file
    DataPreprocess dataPrepro = new DataPreprocess();
    Map<String, String> pidMapping = dataPrepro.getPidMapping(args[0]);
    dataPrepro.createNewFile(pidMapping, args[0]);
  }

  //generate pid mapping
  public Map<String, String> getPidMapping(String path) throws IOException {
    Map<String, String> pidMapping = new HashMap<String, String>();
    BufferedReader br = new BufferedReader(new FileReader(new File(path)));
    String line;

    //get all mapping
    while ((line = br.readLine()) != null) {
      JSONObject obj = new JSONObject(line);
      if (obj.getString("type").equals("delete")) {
        pidMapping.put(obj.getString("pid"), obj.getString("new_id"));
      }
    }

    //remove temporary pid, update to final pid
    Stack<String> tmp = new Stack<String>();
    for (String key : pidMapping.keySet()) {
      while (pidMapping.containsKey(key)) {
        tmp.push(key);
        key = pidMapping.get(key);
      }
      String str;
      while (!tmp.isEmpty()) {
        str = tmp.pop();
        pidMapping.replace(str, key);
      }
    }

    return pidMapping;
  }

  //create new file named "{priorname}.dedup".
  private void createNewFile(Map<String, String> pidMapping, String path) throws IOException {
    BufferedReader br = new BufferedReader(new FileReader(new File(path)));
    String line;
    File newFile = new File(path + Constant.DEDUP);
    OutputStream out = new BufferedOutputStream(new FileOutputStream(newFile));

    while ((line = br.readLine()) != null) {
      JSONObject obj = new JSONObject(line);

      //replace pid use pidmapping
      if (obj.getString("type").equals("visit") && pidMapping.containsKey(obj.getString("pid"))) {
        line =
            line.replaceAll(
                "\"pid\":\"" + obj.getString("pid") + "\"",
                "\"pid\":\"" + pidMapping.get(obj.getString("pid")) + "\"");
      }
      byte[] bytes = (line + System.lineSeparator()).getBytes(Charset.defaultCharset());
      out.write(bytes);
    }

    out.close();
    System.out.println("Preprocess done. New file: " + newFile.getAbsolutePath());
  }
}
