package test;

import cs245.as3.TransactionManager;
import cs245.as3.driver.LeaderboardTests;
import cs245.as3.driver.LogManagerImpl;
import cs245.as3.driver.StorageManagerImpl;
import java.util.Random;
import org.junit.Test;

/**
 * 性能自测
 *
 * @author zjx
 * @since 2022/1/4 上午10:51
 */
public class PerformanceTest {

  //Test seeds will be modified by the autograder
  protected static long[] TEST_SEEDS = new long[] {0x12345671234567L, 0x1000, 42, 9};

  @Test
  public void TestPerfomance() {
    int ans = 0;
    for (int i = 0; i < 100; i ++) {
      ans += LeaderboardTests.TestWriteOps();
    }
    System.out.println(ans / 100);
  }
}
