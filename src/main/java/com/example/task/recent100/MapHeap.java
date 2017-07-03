package com.example.task.recent100;

import java.util.HashMap;
import java.util.Map;

/** reference: http://algs4.cs.princeton.edu/lectures/24PriorityQueues.pdf */
public class MapHeap {

  UserRecord[] heap;
  Map<String, Integer> map;
  int capacity;

  /**
   * Oldest one is at heap peak(heap[1]) For one node at k, parent of k is at k/2. children of k are
   * at 2k and 2k+1.
   *
   * <p>In the map, key is record's pid, and value is index of record in heap.
   */
  public MapHeap(int capacity) {
    this.capacity = capacity;
    heap = new UserRecord[capacity + 1];
    map = new HashMap<String, Integer>();
  }

  public int size() {
    return map.size();
  }

  public void put(UserRecord ur) {
    if (map.containsKey(ur.getPid())) {
      //contained the user visit record
      //check if the new one is later than contained one.
      if (heap[map.get(ur.getPid())].getTime() < ur.getTime()) {
        update(ur);
      }
    } else {
      //not contained the user visit record
      //check if the new one is later than heap peek.

      if (map.size() == capacity && getPeek().getTime() < ur.getTime()) {
        pop();
      }
      if (map.size() < capacity) {
        add(ur);
      }
    }
  }

  //
  public UserRecord getPeek() {
    return heap[1];
  }

  //update one record, then sink.
  private void update(UserRecord ur) {
    heap[map.get(ur.getPid())] = ur;
    sink(map.get(ur.getPid()));
  }

  //pop the oldest one
  public UserRecord pop() {
    if (size() == 0) {
      return null;
    }
    UserRecord tmp = heap[1];
    map.remove(tmp.getPid());
    heap[1] = null;
    heap[1] = heap[size() + 1];
    heap[size() + 1] = null;
    if (getPeek() != null) {
      sink(1);
    }
    return tmp;
  }

  //add the new record in last one, then swim it to correct position
  private void add(UserRecord ur) {
    map.put(ur.getPid(), map.size() + 1);
    heap[map.size()] = ur;
    swim(map.size());
  }

  //check one node with its children, if newer, sink down.
  private void sink(int k) {
    while (2 * k <= map.size()) {
      int j = 2 * k;
      //children of node at k are 2k and 2k+1, find the newer one.
      if (j < map.size() && heap[j].compareTo(heap[j + 1]) > 0) {
        j++;
      }
      if (heap[k].compareTo(heap[j]) <= 0) {
        break;
      }
      exchange(k, j);
      k = j;
    }
  }

  //check with parent, if older, swim up.
  private void swim(int k) {
    while (k > 1 && heap[k].compareTo(heap[k / 2]) < 0) {
      exchange(k, k / 2);
      k = k / 2;
    }
  }

  //exchange two nodes' position
  private void exchange(int m, int n) {
    map.replace(heap[m].getPid(), n);
    map.replace(heap[n].getPid(), m);
    UserRecord tmp = heap[m];
    heap[m] = heap[n];
    heap[n] = tmp;
  }
}
