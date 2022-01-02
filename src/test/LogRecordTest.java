package test;

import cs245.as3.BytesUtils;
import cs245.as3.LogRecord;
import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import org.junit.Test;

/**
 * 日志记录测试
 *
 * @author zjx
 * @since 2022/1/2 下午2:44
 */
public class LogRecordTest {

  private LogRecord constructLogRecord(int type, long txID, long key, byte[] value,
      ArrayList<Long> activeTxns, long activeTxnStartEarlistOffset, int size) {
    LogRecord logRecord = new LogRecord();
    logRecord.setType(type);
    logRecord.setTxID(txID);
    logRecord.setKey(key);
    logRecord.setValue(value);
    logRecord.setActiveTxns(activeTxns);
    logRecord.setActiveTxnStartEarlistOffset(activeTxnStartEarlistOffset);
    logRecord.setSize(size);

    return logRecord;
  }

  @Test
  public void LogRecordTest1() {
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    // start txn
    LogRecord logRecord1 = constructLogRecord(0, 1, 0L, null, null, 0L, 13);
    byte[] b = logRecord1.getByteArray(outputStream);
    assert(logRecord1.equals(BytesUtils.byteToLogRecord(b)));

    // operate
    LogRecord logRecord2 = constructLogRecord(1, 1, 1, "hello world".getBytes(), null, 0L, 32);
    b = logRecord2.getByteArray(outputStream);
    assert(logRecord2.equals(BytesUtils.byteToLogRecord(b)));

    // commit
    LogRecord logRecord3 = constructLogRecord(2, 1, 0L, null, null, 0L, 13);
    b = logRecord3.getByteArray(outputStream);
    assert(logRecord3.equals(BytesUtils.byteToLogRecord(b)));

    // START_CKPT
    LogRecord logRecord4 = constructLogRecord(3, 0L, 0L, null, new ArrayList<>(), 6, 17);
    b = logRecord4.getByteArray(outputStream);
    assert(logRecord4.equals(BytesUtils.byteToLogRecord(b)));

    // END_CKPT
    LogRecord logRecord5 = constructLogRecord(4, 0L, 0L, null, null, 0L, 5);
    b = logRecord5.getByteArray(outputStream);
    assert(logRecord5.equals(BytesUtils.byteToLogRecord(b)));

    // START_CKPT
    ArrayList<Long> arrayList = new ArrayList<>();
    arrayList.add(6L); arrayList.add(8L);
    LogRecord logRecord6 = constructLogRecord(3, 0L, 0L, null, arrayList, 6, 33);
    b = logRecord6.getByteArray(outputStream);
    assert(logRecord6.equals(BytesUtils.byteToLogRecord(b)));
  }
}
