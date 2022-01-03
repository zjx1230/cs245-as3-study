package test;

import cs245.as3.BytesUtils;
import cs245.as3.LogRecord;
import cs245.as3.LogRecordType;
import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import javax.print.attribute.standard.NumberUp;
import org.junit.Test;

/**
 * 日志记录测试
 *
 * @author zjx
 * @since 2022/1/2 下午2:44
 */
public class LogRecordTest {

  private LogRecord constructLogRecord(int type, long txID, long key, byte[] value,
      ArrayList<Long> activeTxns, int activeTxnStartEarlistOffset, int size) {
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
    LogRecord logRecord1 = constructLogRecord(LogRecordType.START_TXN, 1L, 0L, null, null, 0, 13);
    byte[] b = logRecord1.getByteArray(outputStream);
    assert(logRecord1.equals(BytesUtils.byteToLogRecord(b)));

    // operate
    LogRecord logRecord2 = constructLogRecord(LogRecordType.OPERATION, 1L, 1L, "hello world".getBytes(), null, 0, 32);
    b = logRecord2.getByteArray(outputStream);
    assert(logRecord2.equals(BytesUtils.byteToLogRecord(b)));

    // commit
    LogRecord logRecord3 = constructLogRecord(LogRecordType.COMMIT_TXN, 1L, 0L, null, null, 0, 13);
    b = logRecord3.getByteArray(outputStream);
    assert(logRecord3.equals(BytesUtils.byteToLogRecord(b)));

    // START_CKPT
    LogRecord logRecord4 = constructLogRecord(LogRecordType.START_CKPT, 0L, 0L, null, new ArrayList<>(), 6, 17);
    b = logRecord4.getByteArray(outputStream);
    assert(logRecord4.equals(BytesUtils.byteToLogRecord(b)));

    // END_CKPT
    LogRecord logRecord5 = constructLogRecord(LogRecordType.END_CKPT, 0L, 0L, null, null, 0, 5);
    b = logRecord5.getByteArray(outputStream);
    assert(logRecord5.equals(BytesUtils.byteToLogRecord(b)));

    // START_CKPT
    ArrayList<Long> arrayList = new ArrayList<>();
    arrayList.add(6L); arrayList.add(8L);
    LogRecord logRecord6 = constructLogRecord(LogRecordType.START_CKPT, 0L, 0L, null, arrayList, 6, 33);
    b = logRecord6.getByteArray(outputStream);
    assert(logRecord6.equals(BytesUtils.byteToLogRecord(b)));

    // abort
    LogRecord logRecord7 = constructLogRecord(LogRecordType.ABORT_TXN, 1L, 0L, null, null, 0, 13);
    b = logRecord7.getByteArray(outputStream);
    assert(logRecord7.equals(BytesUtils.byteToLogRecord(b)));
  }
}
