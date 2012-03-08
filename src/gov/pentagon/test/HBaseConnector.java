package gov.pentagon.test;

import gov.pentagon.utils.ProjectPreferences;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

public class HBaseConnector {
	/**
	 * Tests your HBase connection.
	 * 
	 * This works only if you have a table called "test", so make sure you do
	 * (you can create a table from within Java I guess, don't know how)
	 * 
	 * Based on {@link http://hbase.apache.org/docs/r0.20.6/api/org/apache/hadoop/hbase/client/package-summary.html#package_description} 
	 * 
	 * @author bocete
	 * @param args
	 * @throws IOException
	 */
	public static void main(String[] args) throws IOException {

		System.out.println("Connecting to HBase...");
		
		Configuration conf = (Configuration) HBaseConfiguration.create();
		conf.addResource(new Path(ProjectPreferences.getPrefs().getProperty(ProjectPreferences.HBASE_CONFIG_PATH)));
		HBaseAdmin admin = new HBaseAdmin(conf);
		System.out.println("I guess it worked?");
		
		System.out.println("Checking if there is a table called AintThisAwesome...");
		if (admin.isTableAvailable("AintThisAwesome"))
			System.out.println("Yes there is!");
		else {
			System.out.println("Nope, creating it...");
			HTableDescriptor tableDescriptor = new HTableDescriptor("AintThisAwesome");
			tableDescriptor.addFamily(new HColumnDescriptor("cf"));
			admin.createTable(tableDescriptor);

			System.out.println("Hopefully done! Remember to run this again to check if the table persisted");
		}
		
		System.out.println("Alright, time to insert and lookup!");
		HTable table = new HTable(conf, "AintThisAwesome");

		// let's add stuff

		Put put = new Put(Bytes.toBytes("myRow")); // row key
		put.add(Bytes.toBytes("cf"), Bytes.toBytes("myColumn"), Bytes.toBytes("value!"));
		table.put(put);

		System.out.println("I guess it was added? Now look it up");

		Get get = new Get(Bytes.toBytes("myRow"));
		Result r = table.get(get);
		byte[] value = r.getValue(Bytes.toBytes("cf"), Bytes.toBytes("myColumn"));

		System.out.println("Is the correct thing read? " + Bytes.toString(value).equals("value!"));
		
		System.out.println("Yay, done!");
	}
}