package gov.pentagon.test;

import gov.pentagon.tl.TransactionalHTableInterface;
import gov.pentagon.utils.ProjectPreferences;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Tests transactions in the following way: it creates a table of with two rows
 * representing users, and a column representing cash those users have.
 * Initially, user1 has 10000 while user2 has 0. All that money is transferred
 * in chunks of 10 through 100 threads running simultaneously, with delays in
 * between operations. If in the end user1 has 0 while user2 has 10000,
 * everything is ok.
 * 
 * @author bocete
 * 
 */
public class ConcurrencyTest {
	final Class<? extends TransactionalHTableInterface> tlClass;

	static final String TABLE_NAME = "test_users";
	static final byte[] COL_FAMILY = Bytes.toBytes("cf");
	static final byte[] COLUMN = Bytes.toBytes("cash");

	static final int THREADS = 50, ITERATIONS_PER_THREAD = 10;

	public ConcurrencyTest(Class<? extends TransactionalHTableInterface> tlClass) {
		super();
		this.tlClass = tlClass;
	}

	long getCash(HTableInterface hTable, String userId) throws IOException {
		Get get = new Get(Bytes.toBytes(userId));
		byte[] value = hTable.get(get).getValue(COL_FAMILY, COLUMN);
		if (value != null)
			return Bytes.toLong(value);
		else
			throw new RuntimeException("Something's wrong, cash read from table is null");
	}

	void setCash(HTableInterface hTable, String userId, long amount) throws IOException {
		Put put = new Put(Bytes.toBytes(userId));
		put.add(COL_FAMILY, COLUMN, Bytes.toBytes(amount));
		hTable.put(put);
	}

	TransactionalHTableInterface createTransactionalTable(Configuration conf) throws Exception {
		Constructor<? extends TransactionalHTableInterface> constructor = tlClass.getConstructor(Configuration.class, String.class);
		return constructor.newInstance(conf, TABLE_NAME);
	}

	Configuration initializeTest() throws Exception {
		// create the cash table
		Configuration conf = (Configuration) HBaseConfiguration.create();
		conf.addResource(new Path(ProjectPreferences.getPrefs().getProperty(ProjectPreferences.HBASE_CONFIG_PATH)));
		HBaseAdmin admin = new HBaseAdmin(conf);

		if (admin.isTableAvailable(TABLE_NAME)) {
			admin.disableTable(TABLE_NAME);
			admin.deleteTable(TABLE_NAME);
		}
		HTableDescriptor tableDescriptor = new HTableDescriptor(TABLE_NAME);
		tableDescriptor.addFamily(new HColumnDescriptor(COL_FAMILY));
		admin.createTable(tableDescriptor);

		TransactionalHTableInterface hTable = createTransactionalTable(conf);
		// set the initial values
		hTable.openTransaction();
		setCash(hTable, "user1", THREADS * ITERATIONS_PER_THREAD * 10);
		setCash(hTable, "user2", 0);
		hTable.commitTransaction();

		// now busy-wait until the initial values are applied
		// Maybe this wait is not necessary? Dunno, don't care to
		// find out
		try {
			hTable.openTransaction();
			while (getCash(hTable, "user1") != THREADS * ITERATIONS_PER_THREAD * 10 || getCash(hTable, "user2") != 0) {
				try {
					Thread.sleep(10);
				} catch (InterruptedException e) {
					throw new RuntimeException("How did this happen?", e);
				}
			}
		} finally {
			hTable.commitTransaction();
		}

		return conf;
	}

	public boolean test() throws Exception {
		// this test will involve many threads transferring money from one row
		// in a table to another, simultaneously.
		// the total amount of money at the end should be the same as what it
		// was at first.

		Configuration conf = initializeTest();

		// makes sure they all start at the same time
		final CyclicBarrier barrier = new CyclicBarrier(THREADS);
		final AtomicInteger threadsDone = new AtomicInteger(THREADS);

		for (int i = 0; i < THREADS; i++) {
			// one for each thread!
			try {
				final TransactionalHTableInterface tlTable = createTransactionalTable(conf);

				new Thread() {
					public void run() {
						try {
							barrier.await();
							for (int i = 0; i < ITERATIONS_PER_THREAD; i++) {
								tlTable.openTransaction();
								long cash1 = getCash(tlTable, "user1");
								Thread.sleep(5);
								long cash2 = getCash(tlTable, "user2");
								Thread.sleep(5);
								setCash(tlTable, "user1", cash1 - 10);
								Thread.sleep(5);
								setCash(tlTable, "user2", cash2 + 10);
								if (!tlTable.commitTransaction()) {
									i--; // repeat cause rollbacked
								}
							}
							synchronized (threadsDone) {
								threadsDone.decrementAndGet();
								threadsDone.notify();
							}
						} catch (Exception e) {
							throw new RuntimeException(e);
						}
					};
				}.start();
			} catch (Exception ex) {
				throw new RuntimeException("Provided transaction layer has to have a constructor using a Configuration and String", ex);
			}
		}

		// wait until the test is complete
		synchronized (threadsDone) {
			while (threadsDone.get() > 0)
				try {
					threadsDone.wait();
				} catch (InterruptedException e) {
					throw new RuntimeException("How did this happen?", e);
				}
		}

		// now wait for some more, just so that we'ew sure all the updates
		// have been applied; maybe not necessary, who cares
		try {
			Thread.sleep(100);
		} catch (InterruptedException e) {
			throw new RuntimeException("How did this happen?", e);
		}

		// finally, read the sums at the end and compare!
		TransactionalHTableInterface table = createTransactionalTable(conf);
		try {
			table.openTransaction();
			System.out.println(getCash(table, "user1"));
			System.out.println(getCash(table, "user2"));
			return getCash(table, "user1") + getCash(table, "user2") == THREADS * ITERATIONS_PER_THREAD * 10;
		} finally {
			table.commitTransaction();
		}
	}

	class ConcurrencyTestRunnable implements Runnable {
		TransactionalHTableInterface tlTable;
		CyclicBarrier barrier;

		public ConcurrencyTestRunnable(TransactionalHTableInterface hTable, CyclicBarrier barrier) {
			super();
			this.tlTable = hTable;
		}

		public void run() {

		}
	}
}
