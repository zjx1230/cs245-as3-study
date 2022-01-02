package cs245.as3;

/**
 * 日志记录类型
 *
 * @author zjx
 * @since 2022/1/2 上午9:57
 */
public class LogRecordType {

  public final static int START_TXN = 0;

  public final static int OPERATION = 1;

  public final static int COMMIT_TXN = 2;

  public final static int START_CKPT = 3;

  public final static int END_CKPT = 4;

  public final static int ABORT_TXN = 5;
}
