package gov.pentagon.tl.impl;

import gov.pentagon.test.ConcurrencyTest;
import gov.pentagon.tl.TransactionalHTableInterface;

import java.io.IOException;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HTable;

/**
 * An implementation of TransactionalHTableInterface that enforces mutual
 * exclusion. The idea is, when transactional code is run using this transaction
 * layer, things will work 100% correct. Of course, this would not work in a
 * distributed system; it's merely here to make sure that our API and test work.
 * 
 * @author bocete
 * 
 */
public class MutexTransactionalHTable extends HTable implements TransactionalHTableInterface {

	public MutexTransactionalHTable(byte[] tableName, HConnection connection, ExecutorService pool) throws IOException {
		super(tableName, connection, pool);
	}

	public MutexTransactionalHTable(byte[] tableName) throws IOException {
		super(tableName);
	}

	public MutexTransactionalHTable(Configuration conf, byte[] tableName) throws IOException {
		super(conf, tableName);
	}

	public MutexTransactionalHTable(Configuration conf, String tableName) throws IOException {
		super(conf, tableName);
	}

	public MutexTransactionalHTable(String tableName) throws IOException {
		super(tableName);
	}

	private static final ReentrantLock LOCK = new ReentrantLock();

	@Override
	public void openTransaction() {
		LOCK.lock();
	}

	@Override
	public boolean commitTransaction() {
		LOCK.unlock();
		return true;
	}
	
	public static void main(String[] args) throws Exception {
		System.out.println(new ConcurrencyTest(MutexTransactionalHTable.class).test());
	}
}
