package com.example.task.recent100;

import junit.framework.TestCase;

public class MapHeapTest extends TestCase {

  private UserRecord[] test = new UserRecord[10];;
  private MapHeap heap = new MapHeap(8);;

  public MapHeapTest(String name) {
    super(name);
  }

  protected void setUp() throws Exception {
    Long time = 100L;
    for (int i = 9; i >= 0; i--) {
      test[i] = new UserRecord(i + "", time++);
    }
  }

  protected void tearDown() throws Exception {
    super.tearDown();
  }

  public void testPop() {
    for (int i = 0; i < heap.capacity; i++) {
      heap.put(test[i]);
    }
    assertEquals(8, heap.size());
    assertEquals("7", heap.pop().getPid());
    assertEquals("6", heap.getPeek().getPid());
    assertEquals(7, heap.size());
  }

  public void testPut() {
    for (int i = 0; i < test.length; i++) {
      assertEquals(Math.min(i, 8), heap.size());
      heap.put(test[i]);
      assertEquals(Math.min(i, 7) + "", heap.getPeek().getPid());
    }
  }
}
