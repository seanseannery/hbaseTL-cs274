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
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;

@Deprecated
public class Attempt1TransactionalHTable extends HTable implements TransactionalHTableInterface {

	public Attempt1TransactionalHTable(byte[] tableName, HConnection connection, ExecutorService pool) throws IOException {
		super(tableName, connection, pool);
	}

	public Attempt1TransactionalHTable(byte[] tableName) throws IOException {
		super(tableName);
	}

	public Attempt1TransactionalHTable(Configuration conf, byte[] tableName) throws IOException {
		super(conf, tableName);
	}

	public Attempt1TransactionalHTable(Configuration conf, String tableName) throws IOException {
		super(conf, tableName);
	}

	public Attempt1TransactionalHTable(String tableName) throws IOException {
		super(tableName);
	}

	private static AtomicInteger transactionIdDispenser = new AtomicInteger();
	private static ConcurrentLinkedQueue<CopyOnWriteArrayList<Object>> transactionStates = new ConcurrentLinkedQueue<CopyOnWriteArrayList<Object>>();
	private static ConcurrentLinkedQueue<ConcurrentHashMap<String, Object>> commitQueue = new ConcurrentLinkedQueue<ConcurrentHashMap<String, Object>>();

	int transactionId;
	int visibleId;

	Map<byte[][], byte[]> writeSet = new HashMap<byte[][], byte[]>();
	Map<byte[][], byte[]> readSet = new HashMap<byte[][], byte[]>();

	@Override
	public void openTransaction() throws Exception {
		writeSet.clear();
		readSet.clear();
		synchronized (transactionIdDispenser) {
			this.transactionId = transactionIdDispenser.addAndGet(2);
			transactionStates.add(new CopyOnWriteArrayList<Object>(new Object[] { transactionId, Boolean.TRUE }));
		}
		for (CopyOnWriteArrayList<Object> row : transactionStates) {
			if (row.get(1) == Boolean.TRUE) {
				visibleId = ((Integer) row.get(0));
			}
		}
		// System.out.println("Transaction #" + transactionId +
		// " reporting, looking at stuff older then " + visibleId);
	}

	static byte[] join(byte[] row, byte[] cf, byte[] column) {
		return Utils.joinByteArrays(row, cf, column);
	}

	byte[] read(byte[] row, byte[] cf, byte[] column) {
		try {
			byte[][] jointId = new byte[][] { row, cf, column };
			if (writeSet.containsKey(jointId))
				return writeSet.get(jointId);
			if (readSet.containsKey(jointId))
				return readSet.get(column);

			Get get = new Get(row);
			get.setTimeRange(0, visibleId);

			get.addColumn(cf, column);
			byte[] value = super.get(get).getValue(cf, column);
			readSet.put(jointId, value);
			return value;
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	void write(byte[] row, byte[] cf, byte[] column, byte[] value) {
		try {
			byte[][] jointId = new byte[][] { row, cf, column };
			writeSet.put(jointId, value);
			Put put = new Put(row, transactionId);
			put.add(cf, column, value);
			super.put(put);
			// System.out.println("Hopefully written " + Bytes.toString(row) +
			// ":" + Bytes.toString(column) + " = " + Bytes.toLong(value) +
			// " in " + transactionId);
			//
			// System.out.println("<");
			// Get get = new Get(row);
			// get.addColumn(cf, column);
			// System.out.println("  " + Bytes.toString(row) + ":" +
			// Bytes.toString(column) + " = " + super.get(get).getValue(cf,
			// column));
			// System.out.println(">");
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
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
		myRowInQueue.put("transId", transactionId);
		Set<byte[][]> myWriteSet = new HashSet<byte[][]>(writeSet.keySet());
		// for performance, copy the writeset keys into a hashset
		myRowInQueue.put("writeSet", myWriteSet);

		commitQueue.add(myRowInQueue);

		boolean doCommit = true;
		for (ConcurrentHashMap<String, Object> rowBeforeMe : commitQueue) {
			if (rowBeforeMe == myRowInQueue)
				break; // committing!
			if (Utils.doSetsOverlap((Set<byte[][]>) rowBeforeMe.get("writeSet"), myWriteSet)) {
				// I am dependent on this guy. I should wait to see if he
				// committed or not
				while (!rowBeforeMe.containsKey("committed"))
					Thread.sleep(10);
				// Time to make the decision...
				if (rowBeforeMe.get("committed") == Boolean.TRUE) {
					doCommit = false;
					break;
				}
			}
		}

		if (!doCommit) {
			Map<byte[], Delete> everythingToBeDeleted = new HashMap<byte[], Delete>();
			for (Map.Entry<byte[][], byte[]> entry : writeSet.entrySet()) {
				byte[] row = entry.getKey()[0];
				Delete delete = everythingToBeDeleted.get(row);
				if (delete == null) {
					delete = new Delete(row);
					delete.setTimestamp(transactionId + 1);
					everythingToBeDeleted.put(row, delete);
				}
				delete.deleteColumn(entry.getKey()[1], entry.getKey()[2]);
			}
			for (Delete delete : everythingToBeDeleted.values())
				super.delete(delete);
		}
		myRowInQueue.put("committed", Boolean.valueOf(doCommit));
		for (CopyOnWriteArrayList<Object> row : transactionStates) {
			if ((Integer) row.get(0) == transactionId) {
				row.set(1, Boolean.FALSE);
			}
		}
		return doCommit;
	}

	public static void main(String[] args) throws Exception {
		System.out.println(new ConcurrencyTest(Attempt1TransactionalHTable.class).test());
	}
}
