package com.constambeys.storm;

import com.constambeys.storm.bolt.BoltProcessor;
import com.constambeys.storm.bolt.PointProcessor;
import com.constambeys.storm.spouts.PointsReader;
import com.constambeys.storm.spouts.SignalsSpout;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
//import backtype.storm.tuple.Fields;

public class TopologyMain {

	public static void main(String[] args) throws InterruptedException {
		try {
			TopologyBuilder builder = new TopologyBuilder();
			builder.setSpout("points-reader", new PointsReader());
			builder.setSpout("signals-spout", new SignalsSpout());

			builder.setBolt("point-processor", new PointProcessor()).shuffleGrouping("points-reader");

			builder.setBolt("k-means-online", new BoltProcessor(3), 1).shuffleGrouping("point-processor")
					.allGrouping("signals-spout", "signals");

			Config conf = new Config();
			conf.registerSerialization(Point.class);
			conf.put("file", args[0]);
			conf.setDebug(false);
			conf.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 1);

			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology("Getting-Started-Toplogie", conf, builder.createTopology());

			// Thread.sleep(4000);
			// cluster.shutdown();

		} catch (Exception ioe) {
			System.out.println("################ Exception thrown ################");
			ioe.printStackTrace();
		}
	}

}
