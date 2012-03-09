package gov.pentagon.tl.impl;

import gov.pentagon.test.ConcurrencyTest;
import gov.pentagon.tl.TransactionalHTableInterface;
import gov.pentagon.utils.Utils;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;

public class OurTransactionalHTable extends HTable implements TransactionalHTableInterface {

	/**
	 * Cache writes, and also allow reading self-writes. It is a
	 * map&lt;join(row, column_family, column), value&gt;&gt;
	 */
	Map<byte[], byte[]> writeSet = new HashMap<byte[], byte[]>();

	/**
	 * Cache reads. It is a map&lt;join(row, column_family, column),
	 * value&gt;&gt;
	 */
	Map<byte[], byte[]> readSet = new HashMap<byte[], byte[]>();

	// in the paper: Wi, Si, Ri, Ci
	//
	// W counter table is replaced with a "transaction_id_dispenser" column
	// inside the Meta table
	long transactionId, readingFromTransactionId, requestOrderId, commitTimestamp;

	final static byte[] META_TABLE_NAME = Bytes.toBytes("tl_meta_meta");
	final static byte[] COMMITTED_TABLE_NAME = Bytes.toBytes("tl_meta_commited");

	static byte[] join(byte[] row, byte[] cf, byte[] column) {
		return Utils.joinByteArrays(row, cf, column);
	}

	void init() throws IOException {
		HBaseAdmin admin = new HBaseAdmin(getConfiguration());
		if (!admin.isTableAvailable(META_TABLE_NAME)) {
			HTableDescriptor tableDescriptor = new HTableDescriptor(META_TABLE_NAME);
			tableDescriptor.addFamily(new HColumnDescriptor("cf"));
			admin.createTable(tableDescriptor);
			Put put = new Put(Bytes.toBytes("meta"));
			put.add(Bytes.toBytes("cf"), Bytes.toBytes("transaction_id_dispenser"), Bytes.toBytes(0l));
			new HTable(getConfiguration(), META_TABLE_NAME).put(put);
		}
		if (!admin.isTableAvailable(COMMITTED_TABLE_NAME)) {
			HTableDescriptor tableDescriptor = new HTableDescriptor(COMMITTED_TABLE_NAME);
			tableDescriptor.addFamily(new HColumnDescriptor("cf"));
			admin.createTable(tableDescriptor);
		}
	}

	byte[] read(byte[] row, byte[] cf, byte[] column) throws Exception {
		byte[] jointId = Utils.joinByteArrays(row, cf, column);
		// read self-written and/or cached values
		if (writeSet.containsKey(jointId))
			return writeSet.get(jointId);

		if (readSet.containsKey(jointId))
			return readSet.get(column);

		// find the transaction that has last committed this value

		// first, we scan only the transactions that I'm reading from
		Scan scan = new Scan(Bytes.toBytes(Long.MAX_VALUE - readingFromTransactionId));

		// searching for the most recent one that has written over this data
		Filter hasWrittenOverWhatever = new SingleColumnValueFilter(Bytes.toBytes("cf"), Utils.joinByteArrays(cf, column), CompareOp.NOT_EQUAL, new byte[0]);
		scan.setFilter(hasWrittenOverWhatever);
		HTable commitTable = new HTable(getConfiguration(), COMMITTED_TABLE_NAME);
		ResultScanner scanner = commitTable.getScanner(scan);
		Result firstRow = scanner.next();

		byte[] value;
		if (firstRow == null) {
			// noone has committed this yet! so return null, we don't support
			// backwards compability with non-transactional stuff
			value = null;
		} else {
			// okay, now we need to read the version by firstTow.getRow()
			Get get = new Get(row);
			get.addColumn(cf, column);
			get.setTimeStamp(Bytes.toLong(firstRow.getRow()));

			value = super.get(get).getValue(cf, column);
		}

		// cache the read value
		readSet.put(jointId, value);
		return value;
	}

	void write(byte[] row, byte[] cf, byte[] column, byte[] value) throws Exception {
		byte[] jointId = Utils.joinByteArrays(row, cf, column);
		
		// cache the written value
		writeSet.put(jointId, value);
				
		// store, using transactionId as timestamp
		Put put = new Put(row, transactionId);
		put.add(cf, column, value);
		super.put(put);
	}

	@Override
	public void openTransaction() throws Exception {
		HTable metaTable = new HTable(getConfiguration(), META_TABLE_NAME);
		// just atomically increment the counter and store the value
		this.transactionId = metaTable.incrementColumnValue(Bytes.toBytes("meta"), Bytes.toBytes("cf"), Bytes.toBytes("transaction_id_dispenser"), 1);

		HTable commitTable = new HTable(getConfiguration(), COMMITTED_TABLE_NAME);
		// the commit table contains rows as Long.MAX_VALUE - transaction_id.
		// So that the most recent transactions come first. Anyway, we the most
		// recent committed transaction's id is in the first row.
		ResultScanner scanner = commitTable.getScanner(new Scan());
		Result firstRow = scanner.next();
		if (firstRow != null) {
			this.readingFromTransactionId = Long.MAX_VALUE - Bytes.toLong(firstRow.getRow());
		} else {
			this.readingFromTransactionId = -1;
		}
	}

	
	
	@Override
	public boolean commitTransaction() {
		enqueueForCommitRequest();
		
		return true;
	}

	void enqueueForCommitRequest() {
		// TODO Auto-generated method stub
		
	}

	public static void main(String[] args) throws Exception {
		System.out.println(new ConcurrencyTest(MutexTransactionalHTable.class).test());
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
}
