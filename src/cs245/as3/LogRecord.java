package cs245.as3;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Objects;

/**
 * 日志记录
 *
 * @author zjx
 * @since 2022/1/2 上午9:32
 */
public class LogRecord {

  /**
   * 日志记录类型
   * 0:表示开始一个事务；1:表示一个操作；2:commit;3:检查点开始;4:检查点结束;5:abort
   */
  private int type; // 用一个字节表示

  /**
   * 事务ID
   */
  private long txID;  // 8字节

  /**
   * 键
   */
  private long key;   // 8字节

  /**
   * 值
   */
  private byte[] value; // 可变

  /**
   * 上一条日志记录偏移量
   */
  private int preOffset; // 4字节

  /**
   * 活跃事务数量 用于生成检查点 具体实现用activeTxns.size()获得
   */
  //private int activeTSize;  // 4字节

  /**
   * 活跃事务ID
   */
  private ArrayList<Long> activeTxns; // 可变

  /**
   * 活跃事务集合中最早开始的偏移量
   */
  private int activeTxnStartEarlistOffset; // 4字节

  /**
   * 日志记录的大小
   */
  private int size; // 4个字节

  /**
   * 在日志中的偏移量，不会序列化
   */
  private volatile int offset;

  public int getSize() {
    return size;
  }

  public void setSize(int size) {
    this.size = size;
  }

  public int getType() {
    return type;
  }

  public void setType(int type) {
    this.type = type;
  }

  public long getTxID() {
    return txID;
  }

  public void setTxID(long txID) {
    this.txID = txID;
  }

  public long getKey() {
    return key;
  }

  public void setKey(long key) {
    this.key = key;
  }

  public byte[] getValue() {
    return value;
  }

  public void setValue(byte[] value) {
    this.value = value;
  }

  public int getOffset() {
    return offset;
  }

  public void setOffset(int offset) {
    this.offset = offset;
  }

  public ArrayList<Long> getActiveTxns() {
    return activeTxns;
  }

  public void setActiveTxns(ArrayList<Long> activeTxns) {
    this.activeTxns = activeTxns;
  }

  public int getActiveTxnStartEarlistOffset() {
    return activeTxnStartEarlistOffset;
  }

  public void setActiveTxnStartEarlistOffset(int activeTxnStartEarlistOffset) {
    this.activeTxnStartEarlistOffset = activeTxnStartEarlistOffset;
  }

  public int getPreOffset() {
    return preOffset;
  }

  public void setPreOffset(int preOffset) {
    this.preOffset = preOffset;
  }

  public byte[] getByteArray(ByteArrayOutputStream arrayOutputStream) {
    arrayOutputStream.write(type);  // 主要是type取值范围比较小可以用1字节
    switch (type) {
      case LogRecordType.START_TXN:
        size = 17; // type + txID + preOffset + size;
        byte[] b = BytesUtils.longToByte(txID);
        arrayOutputStream.write(b, 0, b.length);
        break;
      case LogRecordType.OPERATION:
        size = 25 + value.length; // type + txID + key + value + preOffset + size;
        b = BytesUtils.longToByte(txID);
        arrayOutputStream.write(b, 0, b.length);
        b = BytesUtils.longToByte(key);
        arrayOutputStream.write(b, 0, b.length);
        arrayOutputStream.write(value, 0, value.length);
        break;
      case LogRecordType.COMMIT_TXN:
        size = 17; // type + txID + preOffset + size;
        b = BytesUtils.longToByte(txID);
        arrayOutputStream.write(b, 0, b.length);
        break;
      case LogRecordType.START_CKPT:
        size = activeTxns.size() * 8 + 13; // type + activeTSize * 8 + activeTxnStartEarlistOffset + preOffset + size;
//        b = BytesUtils.intToByte(activeTxns.size());
//        arrayOutputStream.write(b, 0, b.length);
        for (long txnId : activeTxns) {
          b = BytesUtils.longToByte(txnId);
          arrayOutputStream.write(b, 0, b.length);
        }
        b = BytesUtils.intToByte(activeTxnStartEarlistOffset);
        arrayOutputStream.write(b, 0, b.length);
        break;
      case LogRecordType.END_CKPT:
        size = 9; // type + preOffset + size; 1 + 4 + 4
        break;
      default:
        // abort
        size = 17; // type + txnId + preOffset + size 1 + 8 + 4 + 4
        b = BytesUtils.longToByte(txID);
        arrayOutputStream.write(b, 0, b.length);
    }

    byte[] b = BytesUtils.intToByte(preOffset);
    arrayOutputStream.write(b, 0, b.length);
    b = BytesUtils.intToByte(size);
    arrayOutputStream.write(b, 0, b.length);

    // flush
    try {
      arrayOutputStream.flush();
    } catch (IOException e) {
      e.printStackTrace();
    }
    byte[] ans = arrayOutputStream.toByteArray();
    arrayOutputStream.reset();
    return ans;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    LogRecord logRecord = (LogRecord) o;
    return type == logRecord.type &&
        txID == logRecord.txID &&
        key == logRecord.key &&
        activeTxnStartEarlistOffset == logRecord.activeTxnStartEarlistOffset &&
        size == logRecord.size &&
        preOffset == logRecord.preOffset &&
        Arrays.equals(value, logRecord.value) &&
        Objects.equals(activeTxns, logRecord.activeTxns);
  }

  @Override
  public int hashCode() {
    int result = Objects.hash(type, txID, key, activeTxns, activeTxnStartEarlistOffset, preOffset, size);
    result = 31 * result + Arrays.hashCode(value);
    return result;
  }
}
