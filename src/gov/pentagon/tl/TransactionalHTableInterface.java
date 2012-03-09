package gov.pentagon.tl;

import org.apache.hadoop.hbase.client.HTableInterface;

public interface TransactionalHTableInterface extends HTableInterface {
	public void openTransaction() throws Exception;

	public boolean commitTransaction() throws Exception;
}
