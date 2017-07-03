package com.example.task.recent100;

/**
 * UserRecord(pid and last visit timestamp in one record)
 *
 * @author Sha Wang
 * @version 0.1 2017-6-29
 */
public class UserRecord implements Comparable<UserRecord> {
  //user id
  private String pid;
  //user visit time
  private long time;

  //constructor
  public UserRecord(String pid, long time) {
    this.pid = pid;
    this.time = time;
  }

  //getter
  public String getPid() {
    return pid;
  }

  public long getTime() {
    return time;
  }

  //setter
  public void setPid(String pid) {
    this.pid = pid;
  }

  public void setTime(long time) {
    this.time = time;
  }

  public int compareTo(UserRecord other) {
    if (this.getTime() < other.getTime()) {
      return -1;
    } else if (this.getTime() > other.getTime()) {
      return 1;
    } else {
      return 0;
    }
  }
}
