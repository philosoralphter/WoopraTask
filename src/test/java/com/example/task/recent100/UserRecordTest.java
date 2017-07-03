package com.example.task.recent100;

import junit.framework.TestCase;

/** @author shawang */
public class UserRecordTest extends TestCase {

  /* (non-Javadoc)
   * @see junit.framework.TestCase#setUp()
   */
  protected void setUp() throws Exception {
    super.setUp();
  }

  /* (non-Javadoc)
   * @see junit.framework.TestCase#tearDown()
   */
  protected void tearDown() throws Exception {
    super.tearDown();
  }

  public void testCompareTo() {
    UserRecord a = new UserRecord("a", 0L);
    UserRecord b = new UserRecord("b", 2L);
    assertEquals(-1, a.compareTo(b));
    assertEquals(1, b.compareTo(a));
  }
}
