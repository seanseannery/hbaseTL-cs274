package gov.pentagon.tl.impl;

import gov.pentagon.test.ConcurrencyTest;
import gov.pentagon.tl.TransactionalHTableInterface;
import gov.pentagon.utils.Utils;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;

@Deprecated
public class UtilConcurrentTransactionalHTable extends HTable implements TransactionalHTableInterface {

	public UtilConcurrentTransactionalHTable(byte[] tableName, HConnection connection, ExecutorService pool) throws IOException {
		super(tableName, connection, pool);
	}

	public UtilConcurrentTransactionalHTable(byte[] tableName) throws IOException {
		super(tableName);
	}

	public UtilConcurrentTransactionalHTable(Configuration conf, byte[] tableName) throws IOException {
		super(conf, tableName);
	}

	public UtilConcurrentTransactionalHTable(Configuration conf, String tableName) throws IOException {
		super(conf, tableName);
	}

	public UtilConcurrentTransactionalHTable(String tableName) throws IOException {
		super(tableName);
	}

	private static AtomicLong commitQueueIndexDispenser = new AtomicLong();
	private static ConcurrentLinkedQueue<ConcurrentHashMap<String, Object>> commitQueue = new ConcurrentLinkedQueue<ConcurrentHashMap<String, Object>>();

	long visibleId;

	Map<byte[][], byte[]> writeSet = new HashMap<byte[][], byte[]>();

	@Override
	public void openTransaction() throws Exception {
		writeSet.clear();

		visibleId = 0;
		for (ConcurrentHashMap<String, Object> row : commitQueue) {
			int done = (Integer)row.get("done");
			if (done == 2 || done == -1) {
				visibleId = ((Long) row.get("id") + 1);
			} else {
				break;
			}
		}
	}

	static byte[] join(byte[] row, byte[] cf, byte[] column) {
		return Utils.joinByteArrays(row, cf, column);
	}

	byte[] read(byte[] row, byte[] cf, byte[] column) {
		try {
			byte[][] jointId = new byte[][] { row, cf, column };
			if (writeSet.containsKey(jointId))
				return writeSet.get(jointId);

			Get get = new Get(row);
			get.setTimeRange(0, visibleId);

			get.addColumn(cf, column);
			byte[] value = super.get(get).getValue(cf, column);
			return value;
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	void write(byte[] row, byte[] cf, byte[] column, byte[] value) {
		byte[][] jointId = new byte[][] { row, cf, column };
		writeSet.put(jointId, value);
	}

	@Override
	public Result get(final Get get) throws IOException {
		return new Result() {
			@Override
			public byte[] getValue(byte[] family, byte[] qualifier) {
				return read(get.getRow(), family, qualifier);
			}
		};
	}

	@Override
	public void put(Put put) throws IOException {
		// assuming that there's only one cf/column pair in the Put
		List<KeyValue> entryList = put.getFamilyMap().values().iterator().next();
		byte[] cf = entryList.get(0).getFamily();
		byte[] column = entryList.get(0).getQualifier();
		byte[] value = entryList.get(0).getValue();
		write(put.getRow(), cf, column, value);
	}

	@Override
	@SuppressWarnings("unchecked")
	public boolean commitTransaction() throws Exception {
		ConcurrentHashMap<String, Object> myRowInQueue = new ConcurrentHashMap<String, Object>();
		Set<byte[][]> myWriteSet = new HashSet<byte[][]>(writeSet.keySet());
		long queueIndex;

		synchronized (commitQueueIndexDispenser) {
			queueIndex = commitQueueIndexDispenser.addAndGet(2);
			myRowInQueue.put("id", queueIndex);
			myRowInQueue.put("writeSet", myWriteSet);
			myRowInQueue.put("done", 0);
			commitQueue.add(myRowInQueue);
		}

		boolean doCommit = true;
		for (ConcurrentHashMap<String, Object> rowBeforeMe : commitQueue) {
			if (rowBeforeMe == myRowInQueue)
				break; // committing!
			if ((Long) rowBeforeMe.get("id") <= visibleId)
				continue;
			
			if (Utils.doSetsOverlap((Set<byte[][]>) rowBeforeMe.get("writeSet"), myWriteSet)) {
				// I am dependent on this guy. I should wait to see if he
				// committed or not
				int done = (Integer) rowBeforeMe.get("done");
				while (done == 0) {
					Thread.sleep(10);
					done = (Integer) rowBeforeMe.get("done");
				}
				// Time to make the decision...
				if ((Integer)rowBeforeMe.get("done") > 0) {
					doCommit = false;
					break;
				}
			}
		}

		if (doCommit) {
			myRowInQueue.put("done", 1);
			Map<byte[], Put> everythingToBePut = new HashMap<byte[], Put>();
			for (Map.Entry<byte[][], byte[]> entry : writeSet.entrySet()) {
				byte[] row = entry.getKey()[0];
				Put put = everythingToBePut.get(row);
				if (put == null) {
					put = new Put(row, queueIndex);
					everythingToBePut.put(row, put);
				}
				put.add(entry.getKey()[1], entry.getKey()[2], entry.getValue());
			}
			for (Put put : everythingToBePut.values())
				super.put(put);
			myRowInQueue.put("done", 2);
		} else {
			myRowInQueue.put("done", -1);
		}
		
		return doCommit;
	}

	public static void main(String[] args) throws Exception {
		System.out.println(new ConcurrencyTest(UtilConcurrentTransactionalHTable.class).test());
	}
}
