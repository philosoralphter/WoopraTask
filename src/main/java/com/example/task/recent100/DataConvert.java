package com.example.task.recent100;

import org.json.JSONArray;
import org.json.JSONObject;

/**
 * Json convert（use org.json）
 *
 * <p>
 *
 * @author Sha Wang
 * @version 1.0 2017-6-29
 * @see
 */
public class DataConvert {
  //convert one piece of user record
  public UserRecord getLatestRecord(String input) {
    //get JSONObject from input
    JSONObject obj = new JSONObject(input);

    //get userID
    String pid = obj.getString("pid");

    //get max time-stamp of actions
    long time = (long) -1;
    JSONArray actions = obj.getJSONArray("actions");
    if (actions.length() == 0) {
      return null;
    }
    for (int i = 0; i < actions.length(); i++) {
      time = Math.max(time, actions.getJSONObject(i).getLong("time"));
    }

    //create UserRecord and return
    UserRecord record = new UserRecord(pid, time);
    return record;
  }
}
