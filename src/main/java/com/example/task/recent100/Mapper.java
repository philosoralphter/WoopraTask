package com.example.task.recent100;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.net.Socket;
import java.nio.charset.Charset;
import java.util.Locale;
import java.util.Scanner;

/**
 * Mapper
 *
 * @author Sha Wang
 * @version 0.1 2017-6-30
 */
public class Mapper {
  private MapHeap heap;
  private File file;
  private OutputStream out;

  public static void main(String[] args) throws IOException {
    //parser the arguments
    int mapperId = Integer.valueOf(args[0]);
    String serverHost = args[1];
    int serverPort = Integer.valueOf(args[2]);
    String path = args[3];
    int recentNCount = Integer.valueOf(args[4]);

    //start the real job
    Mapper mapper = new Mapper();
    mapper.run(path, serverHost, serverPort, mapperId, recentNCount);
  }

  //run
  public void run(String path, String serverHost, int serverPort, int mapperId, int recentNCount)
      throws IOException {
    //initialize
    setup(path, mapperId, recentNCount);

    //asking for jobs, until no more job
    while (true) {
      String job = commWithSrv(Constant.JOB_REQUEST, serverHost, serverPort);
      if (job.equals(Constant.NO_MORE_JOB)) {
        break;
      } else {
        File file = new File(path + "_" + String.format("%05d", Integer.valueOf(job)));
        BufferedReader br = new BufferedReader(new FileReader(file));
        String line = br.readLine();
        while (line != null) {
          map(line);
          line = br.readLine();
        }
      }
    }

    //final
    cleanup();

    //notify the Master all job done
    commWithSrv(Constant.JOB_FINISHED, serverHost, serverPort);
  }

  //clear
  private void cleanup() throws IOException {
    UserRecord tmp;
    while ((tmp = this.heap.pop()) != null) {
      output(tmp.getPid(), String.valueOf(tmp.getTime()));
    }
    this.out.close();
  }

  //map
  private void map(String input) {
    DataConvert dc = new DataConvert();
    UserRecord ur = dc.getLatestRecord(input);
    if (ur != null) {
      this.heap.put(ur);
    }
  }

  //output
  private void output(String key, String value) throws IOException {
    byte[] bytes = (key + "," + value + System.lineSeparator()).getBytes(Charset.defaultCharset());
    this.out.write(bytes);
  }

  //initialize, new heap and open output stream
  private void setup(String path, int mapperId, int recentNCount) throws IOException {
    this.heap = new MapHeap(recentNCount);
    this.file = new File(path + Constant.MAPPER + mapperId);
    this.out = new BufferedOutputStream(new FileOutputStream(this.file));
  }

  //communicate with Server
  private String commWithSrv(String request, String serverHost, int serverPort) throws IOException {
    Socket socket = new Socket(serverHost, serverPort);
    PrintWriter out = new PrintWriter(socket.getOutputStream(), true);

    Scanner scanner = new Scanner(new BufferedInputStream(socket.getInputStream()), "UTF-8");
    scanner.useLocale(Locale.US);

    // send over socket to server
    out.println(request);

    // get reply from server
    String reply = scanner.nextLine();

    // close IO streams, then socket
    out.close();
    scanner.close();
    socket.close();
    return reply;
  }
}
