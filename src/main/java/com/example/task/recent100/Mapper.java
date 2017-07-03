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

public class Mapper {
  private MapHeap mapHeap;
  private File file;
  private OutputStream out;

  public static void main(String[] args) throws IOException {

    //Constant.CREATE_MAPPER_PROCESS + " " + mapper_Index + " " + ip + " " + port + " " + path);
    Mapper mapper = new Mapper();
    int mapperId = Integer.valueOf(args[0]);
    String serverHost = args[1];
    int serverPort = Integer.valueOf(args[2]);
    String path = args[3];

    mapper.run(path, serverHost, serverPort, mapperId);
  }

  //run
  public void run(String path, String serverHost, int serverPort, int mapperId) throws IOException {
    setup(path, mapperId);
    while (true) {
      String job = commWithSrv(Constant.JOB_REQUEST, serverHost, serverPort);
      if (job.equals(Constant.NO_MORE_JOB)) {
        cleanup();
        commWithSrv(Constant.JOB_FINISHED, serverHost, serverPort);
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
  }

  //clear
  private void cleanup() throws IOException {
    UserRecord tmp;
    while ((tmp = this.mapHeap.pop()) != null) {
      output(tmp.getPid(), String.valueOf(tmp.getTime()));
    }
    this.out.close();
  }

  //map
  private void map(String input) {
    DataConvert dc = new DataConvert();
    UserRecord ur = dc.getLatestRecord(input);
    if (ur != null) {
      this.mapHeap.put(ur);
    }
  }

  //output
  private void output(String key, String value) throws IOException {
    byte[] bytes = (key + "," + value + System.lineSeparator()).getBytes(Charset.defaultCharset());
    this.out.write(bytes);
  }

  //initialize
  private void setup(String path, int mapperId) throws IOException {
    this.mapHeap = new MapHeap(Constant.RECENT_N_COUNT);
    this.file = new File(path + Constant.MAPPER + mapperId);
    this.out = new BufferedOutputStream(new FileOutputStream(this.file));
  }

  //communicate with Server
  private String commWithSrv(String request, String serverHost, int serverPort) throws IOException {
    Socket socket = new Socket(serverHost, serverPort);
    PrintWriter out = new PrintWriter(socket.getOutputStream(), true);

    Scanner scanner = new Scanner(new BufferedInputStream(socket.getInputStream()), "UTF-8");
    scanner.useLocale(Locale.US);

    System.err.println("Connected to " + serverHost + " on port " + serverPort);

    // send over socket to server
    out.println(request);

    // get reply from server
    String reply = scanner.nextLine();

    // close IO streams, then socket
    System.err.println("Closing connection to " + serverHost);
    out.close();
    scanner.close();
    socket.close();
    return reply;
  }
}
