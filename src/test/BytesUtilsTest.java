package test;

import cs245.as3.BytesUtils;
import org.junit.Test;

/**
 * BytesUtils单测
 *
 * @author zjx
 * @since 2022/1/1 下午11:01
 */
public class BytesUtilsTest {

  @Test
  public void BytesUtilsTest1() {
    int a = Integer.MAX_VALUE;
    int b = Integer.MIN_VALUE;
    int c = 0;
    byte[] abytes = BytesUtils.intToByte(a);
    byte[] bbytes = BytesUtils.intToByte(b);
    byte[] cbytes = BytesUtils.intToByte(c);

    assert(a == BytesUtils.byteToInt(abytes));
    assert(b == BytesUtils.byteToInt(bbytes));
    assert(c == BytesUtils.byteToInt(cbytes));

    long d = Long.MAX_VALUE;
    long e = Long.MIN_VALUE;
    long f = 0;
    byte[] dbytes = BytesUtils.longToByte(d);
    byte[] ebytes = BytesUtils.longToByte(e);
    byte[] fbytes = BytesUtils.longToByte(f);

    assert(d == BytesUtils.byteToLong(dbytes));
    assert(e == BytesUtils.byteToLong(ebytes));
    assert(f == BytesUtils.byteToLong(fbytes));
  }
}
