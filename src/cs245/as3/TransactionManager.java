package cs245.as3;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

import cs245.as3.interfaces.LogManager;
import cs245.as3.interfaces.StorageManager;
import cs245.as3.interfaces.StorageManager.TaggedValue;
import java.util.HashSet;
import java.util.Map.Entry;

/**
 * You will implement this class.
 *
 * The implementation we have provided below performs atomic transactions but the changes are not durable.
 * Feel free to replace any of the data structures in your implementation, though the instructor solution includes
 * the same data structures (with additional fields) and uses the same strategy of buffering writes until commit.
 *
 * Your implementation need not be threadsafe, i.e. no methods of TransactionManager are ever called concurrently.
 *
 * You can assume that the constructor and initAndRecover() are both called before any of the other methods.
 */
public class TransactionManager {
	class WritesetEntry {
		public long key;
		public byte[] value;
		public WritesetEntry(long key, byte[] value) {
			this.key = key;
			this.value = value;
		}
	}
	/**
	  * Holds the latest value for each key.
	  */
	private HashMap<Long, TaggedValue> latestValues;
	/**
	  * Hold on to writesets until commit.
	  */
//	private HashMap<Long, ArrayList<WritesetEntry>> writesets;
	private HashMap<Long, HashMap<Long, byte[]> > writesets;

	/**
	 * 活跃的事务集合
	 */
	private HashSet<Long> activeTxns;

	/**
	 * 已经commit但未持久化的事务集合
	 */
	private HashSet<Long> commitTxns;

	/**
	 * 已经持久化的集合
	 */
	private HashSet<Long> persistTxns;

	/**
	 * 做检查点时已经commit但未持久化的事务集合快照
	 */
	private ArrayList<Long> commitTxnsInCkpt;

	private ByteArrayOutputStream bArray;

	/**
	 * 上次日志的大小
	 */
	private int preLogSize;

	/**
	 * 是否正处于一个检查点中
	 */
	private boolean isInCKPT;

	/**
	 * 每个事务的Start出现在日志中的偏移
	 */
	private HashMap<Long, Integer> earlistTxns;

	/**
	 * 每个事务中所操作key的最后一条偏移
	 */
	private HashMap<Long, HashMap<Long, Integer> > txnsOffsetMap;

	/**
	 * 截断点
	 */
	private int trunctionPos;

	/**
	 * 维护上一条日志记录位置
	 */
	private int preLogPos;

	private StorageManager sm;

	private LogManager lm;

	public TransactionManager() {
		writesets = new HashMap<>();
		//see initAndRecover
		latestValues = null;
		bArray = new ByteArrayOutputStream();
		activeTxns = new HashSet<>();
		earlistTxns = new HashMap<>();
		txnsOffsetMap = new HashMap<>();
		commitTxns = new HashSet<>();
		persistTxns = new HashSet<>();
		commitTxnsInCkpt = new ArrayList<>();
		preLogSize = 0;
		isInCKPT = false;
		preLogPos = -1;
	}

	/**
	 * Prepare the transaction manager to serve operations.
	 * At this time you should detect whether the StorageManager is inconsistent and recover it.
	 */
	public void initAndRecover(StorageManager storageManager, LogManager logManager) {
		latestValues = storageManager.readStoredTable();
		sm = storageManager;
		lm = logManager;
		// 获取上一条日志记录位置偏移
		int curPos = lm.getLogEndOffset();
		if (curPos - 4 >= lm.getLogTruncationOffset()) {
			byte[] b = lm.readLogRecord(curPos - 4, 4);
			preLogPos = curPos - BytesUtils.byteToInt(b);
		}

		if (preLogPos == -1) return;

		// 开始从后往前扫描日志
		boolean isEndCkpt = false;	// 用于判断从后往前是否遇到结束检查点的日志
		int pos = preLogPos;
		int limitPos = lm.getLogTruncationOffset();		// 日志当前截断点
		HashSet<Long> needRedoTxns = new HashSet<>();	// 存储需要重做的事务ID
		ArrayList<LogRecord> logRecords = new ArrayList<>();	// 需要重做的日志记录
		int checkLimitPos = -1;	// 日志恢复需要检查的最小位置，有的话就是从后往前扫描第一个与endCkpt记录匹配的startCkpt记录中活跃事务最早start的偏移
		while (pos >= limitPos) {
			if (pos < checkLimitPos) break;
			byte[] b = lm.readLogRecord(pos, curPos - pos);
			LogRecord logRecord = BytesUtils.byteToLogRecord(b);
			switch (logRecord.getType()) {
				case LogRecordType.OPERATION:
					if (needRedoTxns.contains(logRecord.getTxID())) {	// 只存储需要重做的日志记录
						logRecord.setOffset(pos);
						logRecords.add(logRecord);
					}
					break;
				case LogRecordType.COMMIT_TXN:
					if (checkLimitPos == -1) needRedoTxns.add(logRecord.getTxID());
					break;
				case LogRecordType.START_CKPT:
					if (isEndCkpt && checkLimitPos == -1) {
						checkLimitPos = logRecord.getActiveTxnStartEarlistOffset();	// START_CKPT活跃事务中最早start的偏移
					}
					break;
				case LogRecordType.END_CKPT:
					isEndCkpt = true;
					break;
				default:
					// abort do nothing
			}
			curPos = pos;
			pos = logRecord.getPreOffset();
		}

		// 开始重做
		for (int i = logRecords.size() - 1; i >= 0; i --) {
			LogRecord logRecord = logRecords.get(i);
			sm.queueWrite(logRecord.getKey(), logRecord.getOffset(), logRecord.getValue());
			latestValues.put(logRecord.getKey(), new TaggedValue(logRecord.getOffset(), logRecord.getValue()));
		}
	}

	/**
	 * Indicates the start of a new transaction. We will guarantee that txID always increases (even across crashes)
	 */
	public void start(long txID) {
		// 先写日志
		LogRecord logRecord = new LogRecord();
		logRecord.setType(LogRecordType.START_TXN);
		logRecord.setTxID(txID);
		logRecord.setPreOffset(preLogPos);
		preLogPos = lm.appendLogRecord(logRecord.getByteArray(bArray));
		activeTxns.add(txID);
		earlistTxns.put(txID, preLogPos);
		//doCheckPoint();
	}

	/**
	 * Returns the latest committed value for a key by any transaction.
	 */
	public byte[] read(long txID, long key) {
		TaggedValue taggedValue = latestValues.get(key);
		//doCheckPoint();
		return taggedValue == null ? null : taggedValue.value;
	}

	/**
	 * Indicates a write to the database. Note that such writes should not be visible to read()
	 * calls until the transaction making the write commits. For simplicity, we will not make reads
	 * to this same key from txID itself after we make a write to the key.
	 */
	public void write(long txID, long key, byte[] value) {
//		ArrayList<WritesetEntry> writeset = writesets.get(txID);
//		if (writeset == null) {
//			writeset = new ArrayList<>();
//			writesets.put(txID, writeset);
//		}
//		writeset.add(new WritesetEntry(key, value));
		HashMap<Long, byte[]> writeset = writesets.getOrDefault(txID, new HashMap<>());
		writeset.put(key, value);
		writesets.put(txID, writeset);

		doCheckPoint();
	}

	/**
	 * 开始执行检查点
	 */
	private void doCheckPoint() {
		// 做检查点
		int logSize = lm.getLogEndOffset();

		//System.out.println("logSize : " + logSize + " preLogSize: " + preLogSize);
		if (!isInCKPT && preLogSize != 0 && logSize - preLogSize > 300) {
			preLogSize = logSize;
			//System.out.println("logSize : " + logSize + " preLogSize: " + preLogSize);
			// 添加检查点日志记录
			LogRecord logRecord = new LogRecord();
			ArrayList<Long> txns = new ArrayList<>();
			int earlistLogSize = logSize + 1;
			for (Long txnId : activeTxns) {
				earlistLogSize = Math.min(earlistLogSize, earlistTxns.get(txnId));
				txns.add(txnId);
			}
			if (earlistLogSize != logSize + 1) trunctionPos = earlistLogSize;
			logRecord.setActiveTxns(txns);
			logRecord.setActiveTxnStartEarlistOffset(earlistLogSize);
			logRecord.setType(LogRecordType.START_CKPT);
			logRecord.setPreOffset(preLogPos);
			preLogPos = lm.appendLogRecord(logRecord.getByteArray(bArray));
			isInCKPT = true;

			commitTxnsInCkpt.clear();	// 清除之前的
			for (long txnId : commitTxns) {
				commitTxnsInCkpt.add(txnId);
			}
		}
		if (preLogSize == 0) {
			preLogSize = logSize;
		}
	}

	/**
	 * Commits a transaction, and makes its writes visible to subsequent read operations.\
	 */
	public void commit(long txID) {
		activeTxns.remove(txID);	// 已提交，则从当前活跃事务集合中删掉
		commitTxns.add(txID);			// 添加到已提交集合中

//		ArrayList<WritesetEntry> writeset = writesets.get(txID);
		HashMap<Long, byte[]> writeset = writesets.get(txID);
		ArrayList<LogRecord> logRecords = new ArrayList<>();
		if (writeset != null) {
//			for(WritesetEntry x : writeset) {
			for (Entry<Long, byte[]> x: writeset.entrySet()) {
				// 添加日志
				LogRecord logRecord = new LogRecord();
				logRecord.setTxID(txID);
				logRecord.setType(LogRecordType.OPERATION);
				logRecord.setKey(x.getKey());
				logRecord.setValue(x.getValue());
				logRecord.setPreOffset(preLogPos);
				preLogPos = lm.appendLogRecord(logRecord.getByteArray(bArray));
				latestValues.put(x.getKey(), new TaggedValue(preLogPos, x.getValue()));
				HashMap<Long, Integer> txn = txnsOffsetMap.getOrDefault(txID, new HashMap<>());
				txn.put(x.getKey(), preLogPos);
				txnsOffsetMap.put(txID, txn);
				logRecord.setOffset(preLogPos);
				logRecords.add(logRecord);
			}
			writesets.remove(txID);
		}
		// 添加日志
		LogRecord logRecord = new LogRecord();
		logRecord.setTxID(txID);
		logRecord.setType(LogRecordType.COMMIT_TXN);
		logRecord.setTxID(txID);
		logRecord.setPreOffset(preLogPos);
		preLogPos = lm.appendLogRecord(logRecord.getByteArray(bArray));

		// 先写日志再写数据到sm
		for (LogRecord logRecord1 : logRecords) {
			sm.queueWrite(logRecord1.getKey(), logRecord1.getOffset(), logRecord1.getValue());	// 异步的
		}

		//doCheckPoint();
	}
	/**
	 * Aborts a transaction.
	 */
	public void abort(long txID) {
		writesets.remove(txID);
		activeTxns.remove(txID);
		//doCheckPoint();
	}

	/**
	 * The storage manager will call back into this procedure every time a queued write becomes persistent.
	 * These calls are in order of writes to a key and will occur once for every such queued write, unless a crash occurs.
	 */
	public void writePersisted(long key, long persisted_tag, byte[] persisted_value) {	// 表明该数据已经持久化了
		doCheckPoint();
		// 根据持久化的数据判断当前以提交集合事务中有哪些事务已经持久化了
		ArrayList<Long> txns = new ArrayList<>();
		for (Long txnId : commitTxns) {
			if (txnsOffsetMap.containsKey(txnId)) {
				if (txnsOffsetMap.get(txnId).containsKey(key)
						&& txnsOffsetMap.get(txnId).get(key) <= persisted_tag) {
					txnsOffsetMap.get(txnId).remove(key);
				}
				if (txnsOffsetMap.get(txnId).size() == 0) { // txnId的事务已经没有待持久化的数据
					txns.add(txnId);
					persistTxns.add(txnId);
				}
			}
		}

		for (Long txnId : txns) {
			commitTxns.remove(txnId);
		}

		// 当前处于检查点阶段且检查点建立时提交的事务已经持久化，则结束检查点阶段
		if (isInCKPT && canEndCkpt()) {
			// 添加检查点结束日志记录
			LogRecord logRecord = new LogRecord();
			logRecord.setType(LogRecordType.END_CKPT);
			logRecord.setPreOffset(preLogPos);
			preLogPos = lm.appendLogRecord(logRecord.getByteArray(bArray));
			lm.setLogTruncationOffset(trunctionPos);	// 截断用于快速恢复
			isInCKPT = false;
		}
	}

	/**
	 * 判断是否可以结束检查点操作
	 * 如果建立检查点时已commit事务集合已经都持久化好则可以执行结束检查点操作
	 * @return true 表示是， false 表示否
	 */
	private boolean canEndCkpt() {
		for (long txnId : commitTxnsInCkpt) {
			if (!persistTxns.contains(txnId)) {
				return false;
			}
		}
		return true;
	}
}
