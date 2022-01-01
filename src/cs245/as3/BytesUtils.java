package cs245.as3;

/**
 * 类型转成字节数组的工具类
 *
 * @author zjx
 * @since 2022/1/1 下午10:48
 */
public class BytesUtils {

  /**
   * IntToByte
   * @param res
   * @return
   */
  public static byte[] intToByte(int res) {
    byte[] targets = new byte[4];
    targets[0] = (byte) (res & 0xff);         // 最低位
    targets[1] = (byte) ((res >> 8) & 0xff);  // 次低位
    targets[2] = (byte) ((res >> 16) & 0xff); // 次高位
    targets[3] = (byte) (res >>> 24);         // 最高位,无符号右移。
    return targets;
  }

  /**
   * byteToInt
   * @param arr
   * @return
   */
  public static int byteToInt(byte[] arr) {
    int i0 = (int) ((arr[0] & 0xff) << 0 * 8);
    int i1 = (int) ((arr[1] & 0xff) << 1 * 8);
    int i2 = (int) ((arr[2] & 0xff) << 2 * 8);
    int i3 = (int) ((arr[3] & 0xff) << 3 * 8);
    return i0 + i1 + i2 + i3;
  }

  /**
   * longToByte
   * @param res
   * @return
   */
  public static byte[] longToByte(long res) {
    byte[] targets = new byte[8];
    targets[0] = (byte) (res & 0xff);
    targets[1] = (byte) ((res >> 8) & 0xff);
    targets[2] = (byte) ((res >> 16) & 0xff);
    targets[3] = (byte) ((res >> 24) & 0xff);
    targets[4] = (byte) ((res >> 32) & 0xff);
    targets[5] = (byte) ((res >> 40) & 0xff);
    targets[6] = (byte) ((res >> 48) & 0xff);
    targets[7] = (byte) (res >>> 56);
    return targets;
  }

  /**
   * byteToLong
   * @param arr
   * @return
   */
  public static long byteToLong(byte[] arr) {
    long i0 = (long) (((long)(arr[0] & 0xff)) << 0 * 8);
    long i1 = (long) (((long)(arr[1] & 0xff)) << 1 * 8);
    long i2 = (long) (((long)(arr[2] & 0xff)) << 2 * 8);
    long i3 = (long) (((long)(arr[3] & 0xff)) << 3 * 8);
    long i4 = (long) (((long)(arr[4] & 0xff)) << 4 * 8);
    long i5 = (long) (((long)(arr[5] & 0xff)) << 5 * 8);
    long i6 = (long) (((long)(arr[6] & 0xff)) << 6 * 8);
    long i7 = (long) (((long)(arr[7] & 0xff)) << 7 * 8);
    return i0 + i1 + i2 + i3 + i4 + i5 + i6 + i7;
  }
}
