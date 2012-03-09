package gov.pentagon.tl.impl;

import gov.pentagon.test.ConcurrencyTest;
import gov.pentagon.tl.TransactionalHTableInterface;

import java.io.IOException;
import java.util.concurrent.ExecutorService;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HTable;

/**
 * An implementation of TransactionalHTableInterface that doesn't actually
 * enforce anything. The idea is, when transactional code is run using this
 * transaction layer, things will break.
 * 
 * @author bocete
 * 
 */
public class DummyTransactionalHTable extends HTable implements TransactionalHTableInterface {

	public DummyTransactionalHTable(byte[] tableName, HConnection connection, ExecutorService pool) throws IOException {
		super(tableName, connection, pool);
	}

	public DummyTransactionalHTable(byte[] tableName) throws IOException {
		super(tableName);
	}

	public DummyTransactionalHTable(Configuration conf, byte[] tableName) throws IOException {
		super(conf, tableName);
	}

	public DummyTransactionalHTable(Configuration conf, String tableName) throws IOException {
		super(conf, tableName);
	}

	public DummyTransactionalHTable(String tableName) throws IOException {
		super(tableName);
	}

	@Override
	public void openTransaction() {
		// do nothing
	}

	@Override
	public boolean commitTransaction() {
		// do nothing and accept
		return true;
	}
	
	public static void main(String[] args) throws Exception {
		System.out.println(new ConcurrencyTest(DummyTransactionalHTable.class).test());
	}

}
