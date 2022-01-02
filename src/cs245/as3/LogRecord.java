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

//  /**
//   * 上一条日志记录偏移量
//   */
//  private long preOffset; // 8字节

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
  private long activeTxnStartEarlistOffset; // 8字节

  /**
   * 日志记录的大小
   */
  private int size; // 4个字节

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

//  public int getActiveTSize() {
//    return activeTSize;
//  }
//
//  public void setActiveTSize(int activeTSize) {
//    this.activeTSize = activeTSize;
//  }

  public ArrayList<Long> getActiveTxns() {
    return activeTxns;
  }

  public void setActiveTxns(ArrayList<Long> activeTxns) {
    this.activeTxns = activeTxns;
  }

  public long getActiveTxnStartEarlistOffset() {
    return activeTxnStartEarlistOffset;
  }

  public void setActiveTxnStartEarlistOffset(long activeTxnStartEarlistOffset) {
    this.activeTxnStartEarlistOffset = activeTxnStartEarlistOffset;
  }

  public byte[] getByteArray(ByteArrayOutputStream arrayOutputStream) {
    arrayOutputStream.write(type);  // 主要是type取值范围比较小可以用1字节
    switch (type) {
      case LogRecordType.START_TXN:
        size = 1 + 8 + 4; // type + txID + size;
        byte[] b = BytesUtils.longToByte(txID);
        arrayOutputStream.write(b, 0, b.length);
        break;
      case LogRecordType.OPERATION:
        size = 1 + 8 + 8 + value.length + 4; // type + txID + key + value + size;
        b = BytesUtils.longToByte(txID);
        arrayOutputStream.write(b, 0, b.length);
        b = BytesUtils.longToByte(key);
        arrayOutputStream.write(b, 0, b.length);
        arrayOutputStream.write(value, 0, value.length);
        break;
      case LogRecordType.COMMIT_TXN:
        size = 1 + 8 + 4; // type + txID + size;
        b = BytesUtils.longToByte(txID);
        arrayOutputStream.write(b, 0, b.length);
        break;
      case LogRecordType.START_CKPT:
        size = 1 + 4 + activeTxns.size() * 8 + 8 + 4; // type + activeTSize + activeTSize * 8 + activeTxnStartEarlistOffset + size;
        b = BytesUtils.intToByte(activeTxns.size());
        arrayOutputStream.write(b, 0, b.length);
        for (long txnId : activeTxns) {
          b = BytesUtils.longToByte(txnId);
          arrayOutputStream.write(b, 0, b.length);
        }
        b = BytesUtils.longToByte(activeTxnStartEarlistOffset);
        arrayOutputStream.write(b, 0, b.length);
        break;
      case LogRecordType.END_CKPT:
        size = 1 + 4; // type + size;
        break;
      default:
        // abort do nothing
    }

    byte[] b = BytesUtils.intToByte(size);
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
        Arrays.equals(value, logRecord.value) &&
        Objects.equals(activeTxns, logRecord.activeTxns);
  }

  @Override
  public int hashCode() {
    int result = Objects.hash(type, txID, key, activeTxns, activeTxnStartEarlistOffset, size);
    result = 31 * result + Arrays.hashCode(value);
    return result;
  }
}
