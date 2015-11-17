package com.constambeys.storm;

/**
 * Created by nickozoulis on 23/10/2015.
 */
public class Cons {

	// HBase Configuration constants
	// cd C:\Windows\System32\Drivers\etc
	// # localhost name resolution is handled within DNS itself.
	// 127.0.0.1 localhost
	// 100.74.13.25 debian
	// # ::1 localhost

	// HBASE
	// usr/lib/hbase/hbase-0.98.15-hadoop2/bin
	// .hbase shell
	// list
	// scan 'table name'

	public static final String hbase_IP_address = "127.0.0.1";
	public static final String hbase_port = "2181";

	// HBase table constants
	public static final String queries = "queries";
	public static final String batch_views = "batch_views";
	public static final String stream_views = "stream_views";
	public static final String cfQueries = "q"; // column family for Queries table
	public static final String cfViews = "v"; // column family for batch and stream Views
	public static final String qid_ = "qid_";
	public static final String qid_0 = "qid_0";
	public static final String max_qid = "max_qid";
	public static final String clusters = "clusters";
	public static final String clusters_ = "c_";
	public static final String filter = "filter";

}
