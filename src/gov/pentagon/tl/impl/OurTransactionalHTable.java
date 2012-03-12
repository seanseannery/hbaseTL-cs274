package gov.pentagon.tl.impl;

import gov.pentagon.test.ConcurrencyTest;
import gov.pentagon.tl.TransactionalHTableInterface;
import gov.pentagon.utils.Utils;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ExecutorService;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Our implementation!
 * 
 * The first thing is, our transactions are invisible to the outside world
 * before the validation & commit phases. They're local, and any writes are
 * "buffered" in memory instead of reaching into the database. All reads are
 * written down as well, as they're required for validation. For now, take for
 * granted that values seen by reads have versions that were most recent at the
 * starting time of the transaction.
 * 
 * When a transaction decides to commit, it enters the queue of finished
 * transactions. Its entry in this queue contains the transaction's write set,
 * which is immutable at this point, and also the "status". There are four
 * possible values for this status: Finished, Committing, Committed, and
 * Aborted. In code, these are the numbers 0, 1, 2, -1 in that order. Initially,
 * status is Finished.
 * 
 * After this point the transaction iterates through this queue, looking at all
 * the transactions entered the queue before it and after the transaction
 * started. If there is a transaction which write set overlaps with my read set,
 * then: if that transaction's status is Committing or Committed, this
 * transaction should abort. If it's status is Aborted, proceed as normal. If
 * it's Finished, we need to wait to get resolved to any of the other three.
 * 
 * After that: if we decided to abort, make the status Aborted and that's it.
 * Otherwise, mark the status as Committing, apply all changes that were
 * recorded using the queue index as version, and then mark the status as
 * Committed.
 * 
 * Now back to the issue of which version is visible. The most recent one that's
 * created by transactions that are either Committed or Aborted. To get there,
 * we scan the queue: the first transaction we find that's Finished may be a
 * problem and that is the point at which we draw the line; all versions up to
 * that transaction's id are ok. If we see an Committing transaction, we can
 * either wait for it to complete or just take that as the limit.
 * 
 * 
 * @author bocete
 * 
 */
public class OurTransactionalHTable extends HTable implements TransactionalHTableInterface {

	/**
	 * The name of the HBase table containing our metadata
	 */
	final static byte[] META_TABLE_NAME = Bytes.toBytes("tl_meta_meta");
	/**
	 * The identifier of the only row in our Metadata table
	 */
	final static byte[] META_ROW_ID = Bytes.toBytes("the_only_row");
	/**
	 * The identifier of a column containing a long, pointing to (most often)
	 * the last row in the commit queue
	 */
	final static byte[] META_COLUMN_QUEUE_ID_DISPENSER = Bytes.toBytes("queue_id_dispenser");

	/**
	 * The HBase table name of our commit queue
	 */
	final static byte[] COMMITTED_TABLE_NAME = Bytes.toBytes("tl_meta_committed");
	/**
	 * We only use a single column family in both the commit queue table and the
	 * meta table, this is its identifier
	 */
	final static byte[] DEFAULT_CF = Bytes.toBytes("cf");
	/**
	 * The identifier for the status column in the commit queue
	 */
	final static byte[] COMMITTED_COLUMN_STATUS = Bytes.toBytes("status");

	/**
	 * Write set of the ongoing transaction. In form of Map&lt;[row, tableName,
	 * columnFamily], value&gt; where all four are just byte arrays
	 */
	Map<byte[][], byte[]> writeSet = new HashMap<byte[][], byte[]>();
	/**
	 * Read set, containing only the concatenated arrays of [row, tableName,
	 * columnFamily]
	 */
	Set<byte[]> readKeys = new HashSet<byte[]>();

	/**
	 * The largest visible version number, exclusive.
	 */
	long visibleVersion;

	@Override
	public void openTransaction() throws Exception {
		// clear the read and write sets, this is a new transaction!
		writeSet.clear();
		readKeys.clear();

		// searching for the latest visible version
		// by iterating through the queue in order, stopping whenever
		// we find a row in the queue that's neither Aborted or Committed
		visibleVersion = 0;
		Scan scan = new Scan();
		scan.addColumn(DEFAULT_CF, COMMITTED_COLUMN_STATUS);
		ResultScanner scanner = null;
		try {
			scanner = new HTable(COMMITTED_TABLE_NAME).getScanner(scan);
			for (Result result : scanner) {
				byte done = result.getValue(DEFAULT_CF, COMMITTED_COLUMN_STATUS)[0];
				if (done == -1 || done == 2) {
					visibleVersion = Bytes.toLong(result.getRow()) + 1;
				} else {
					break;
				}
			}
		} finally {
			if (scanner != null)
				scanner.close();
		}
	}

	/**
	 * Helper method that reads the data of the proper version and updates the
	 * readset. This method is supposed to be called from all read methods of
	 * <code>HTableInterface</code>, though we only call it from
	 * <code>get</code> for brevity.
	 * 
	 * @param row
	 *            Row to be read from
	 * @param cf
	 *            Column Family to be read from
	 * @param column
	 *            Column to read from
	 * @return Read value (may be null)
	 */
	byte[] read(byte[] row, byte[] cf, byte[] column) {
		try {
			byte[][] jointId = new byte[][] { row, cf, column };

			// mark that this data has been read
			readKeys.add(Utils.joinByteArrays(jointId));
			// read the written version if one exists
			if (writeSet.containsKey(jointId))
				return writeSet.get(jointId);

			// else, get from the database using the visibleVersion
			Get get = new Get(row);
			get.setTimeRange(0, visibleVersion);

			get.addColumn(cf, column);
			byte[] value = super.get(get).getValue(cf, column);
			return value;
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	/**
	 * Utility method for writing. Basically only updates the write set of the
	 * transaction, but also inserting it in the read set so that collisions are
	 * easier to detect.
	 * 
	 * @param row
	 *            Row to write to
	 * @param cf
	 *            Column family to write to
	 * @param column
	 *            Column to write to
	 * @param value
	 *            Value to write
	 */
	void write(byte[] row, byte[] cf, byte[] column, byte[] value) {
		byte[][] jointId = new byte[][] { row, cf, column };
		readKeys.add(Utils.joinByteArrays(jointId));
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
		// only works assuming that there's only one cf/column pair in the Put
		List<KeyValue> entryList = put.getFamilyMap().values().iterator().next();
		byte[] cf = entryList.get(0).getFamily();
		byte[] column = entryList.get(0).getQualifier();
		byte[] value = entryList.get(0).getValue();
		write(put.getRow(), cf, column, value);
	}

	@Override
	public boolean commitTransaction() throws Exception {
		// set some utility stuff
		HTable metaTable = new HTable(getConfiguration(), META_TABLE_NAME);
		HTable commitedQueue = new HTable(COMMITTED_TABLE_NAME);
		Put put;

		// append this commit to the end of the commit queue
		// this is done by looking up the lastQueueIndex (the number of the
		// "slot" this commit is going into), creating an entry, and attempting
		// to insert the
		// entry in the given slot. That will fail if the slot turns out to be
		// taken, in which case we try it again but this time using
		// max(prevQueueIndex+1, newlyReadLastQueueIndex).
		// only when insertion succeeds is the lastQueueIndex incremented
		long queueIndex = -1;
		long lastReadQueueIndex;
		while (true) {
			// reading the lastQueueIndex
			Get get = new Get(META_ROW_ID);
			get.addColumn(DEFAULT_CF, META_COLUMN_QUEUE_ID_DISPENSER);
			lastReadQueueIndex = Bytes.toLong(metaTable.get(get).getValue(DEFAULT_CF, META_COLUMN_QUEUE_ID_DISPENSER));
			queueIndex = Math.max(lastReadQueueIndex, queueIndex);

			// creating the entry
			put = new Put(Bytes.toBytes(queueIndex));
			put.add(DEFAULT_CF, COMMITTED_COLUMN_STATUS, new byte[] { 0 });
			for (byte[][] writtenTo : writeSet.keySet()) {
				put.add(DEFAULT_CF, Utils.joinByteArrays(writtenTo[0], writtenTo[1], writtenTo[2]), Bytes.toBytes(true));
			}
			// try to put it there; if fails, repeat with a larger queueIndex
			if (commitedQueue.checkAndPut(Bytes.toBytes(queueIndex), DEFAULT_CF, COMMITTED_COLUMN_STATUS, null, put)) {
				break;
			} else
				queueIndex++;
		}
		metaTable.incrementColumnValue(META_ROW_ID, DEFAULT_CF, META_COLUMN_QUEUE_ID_DISPENSER, 1);

		// now that the row is in the queue, we search for conflicts
		// for each transaction whose write set has a common element with my read set,
		// if it's committing or committed we abort. If it's Finished but yet unresolved to commit status,
		// we wait until we know the outcome
		
		// needToCheck contains all the queue indexes that need to be checked for collisions
		Set<Long> needToCheck = new TreeSet<Long>();
		for (long id = visibleVersion; id < queueIndex; id++)
			needToCheck.add(id);
		
		boolean doAbort = false;
		while (doAbort == false && !needToCheck.isEmpty()) {
			// for each id in needToCheck, check for collisions; if a collision is found,
			// set doAbort to true and break. Else just keep going till needToCheck is empty
			Iterator<Long> iterator = needToCheck.iterator();
			while (iterator.hasNext()) {
				long id = iterator.next();
				Get get = new Get(Bytes.toBytes(id));
				Result result = commitedQueue.get(get);
				
				// first, is there a conflict?
				boolean conflict = false;
				for (byte[] readEntry : readKeys)
					if (result.containsColumn(DEFAULT_CF, readEntry)) {
						conflict = true;
						break;
					}
				// if there is a conflict, was that transaction aborted or what?
				if (conflict) {
					byte done = result.getValue(DEFAULT_CF, COMMITTED_COLUMN_STATUS)[0];
					if (done == 0)
						// we need to wait for it to get resolved
						// it will stay in needToCheck and we'll get back to it later
						continue;
					if (done > 0) {
						doAbort = true;
						break;
					} else {
						// it aborted, phew! we can proceed
						iterator.remove();
					}
				} else {
					// no conflict, we can proceed regardless of it's commit state
					iterator.remove();
				}
			}
		}

		if (!doAbort) {
			// no conflicts? mark as 1 (Committing), apply changes, mark as 2 (Committed)
			put = new Put(Bytes.toBytes(queueIndex));
			put.add(DEFAULT_CF, COMMITTED_COLUMN_STATUS, new byte[] { 1 });
			commitedQueue.put(put);

			Map<byte[], Put> everythingToBePut = new HashMap<byte[], Put>();
			for (Map.Entry<byte[][], byte[]> entry : writeSet.entrySet()) {
				byte[] row = entry.getKey()[0];
				put = everythingToBePut.get(row);
				if (put == null) {
					put = new Put(row, queueIndex);
					everythingToBePut.put(row, put);
				}
				put.add(entry.getKey()[1], entry.getKey()[2], entry.getValue());
			}
			for (Put myPut : everythingToBePut.values())
				super.put(myPut);

			put = new Put(Bytes.toBytes(queueIndex));
			put.add(DEFAULT_CF, COMMITTED_COLUMN_STATUS, new byte[] { 2 });
			commitedQueue.put(put);
		} else {
			// else just mark as aborted and return
			put = new Put(Bytes.toBytes(queueIndex));
			put.add(DEFAULT_CF, COMMITTED_COLUMN_STATUS, new byte[] { -1 });
			commitedQueue.put(put);
		}

		return !doAbort;
	}

	public OurTransactionalHTable(byte[] tableName, HConnection connection, ExecutorService pool) throws IOException {
		super(tableName, connection, pool);
		init();
	}

	public OurTransactionalHTable(byte[] tableName) throws IOException {
		super(tableName);
		init();
	}

	public OurTransactionalHTable(Configuration conf, byte[] tableName) throws IOException {
		super(conf, tableName);
		init();
	}

	public OurTransactionalHTable(Configuration conf, String tableName) throws IOException {
		super(conf, tableName);
		init();
	}

	public OurTransactionalHTable(String tableName) throws IOException {
		super(tableName);
		init();
	}

	void init() throws IOException {
		HBaseAdmin admin = new HBaseAdmin(getConfiguration());
		if (!admin.isTableAvailable(META_TABLE_NAME)) {
			HTableDescriptor tableDescriptor = new HTableDescriptor(META_TABLE_NAME);
			tableDescriptor.addFamily(new HColumnDescriptor(DEFAULT_CF));
			admin.createTable(tableDescriptor);
			Put put = new Put(META_ROW_ID);
			put.add(DEFAULT_CF, META_COLUMN_QUEUE_ID_DISPENSER, Bytes.toBytes(0l));
			new HTable(getConfiguration(), META_TABLE_NAME).put(put);
		}
		if (!admin.isTableAvailable(COMMITTED_TABLE_NAME)) {
			HTableDescriptor tableDescriptor = new HTableDescriptor(COMMITTED_TABLE_NAME);
			tableDescriptor.addFamily(new HColumnDescriptor(DEFAULT_CF));
			admin.createTable(tableDescriptor);
		}
	}

	public static void main(String[] args) throws Exception {
		System.out.println(new ConcurrencyTest(OurTransactionalHTable.class).test());
	}
}
