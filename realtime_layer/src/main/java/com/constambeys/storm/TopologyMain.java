package com.constambeys.storm;

import com.constambeys.storm.bolt.BoltOutput;
import com.constambeys.storm.bolt.BoltProcessor;
import com.constambeys.storm.bolt.PointProcessor;
import com.constambeys.storm.bolt.PointsReader;
import com.constambeys.storm.spouts.CommandsSpout;
import com.constambeys.storm.spouts.SignalsSpout;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.BoltDeclarer;
import backtype.storm.topology.TopologyBuilder;

public class TopologyMain {

	public static void main(String[] args) throws InterruptedException {
		try {
			//java -cp realtime_layer-1.0-SNAPSHOT-jar-with-dependencies.jar com.constambeys.storm.TopologyMain
			TopologyBuilder builder = new TopologyBuilder();
			builder.setSpout("signals-spout", new SignalsSpout());
			builder.setSpout("commands-spout", new CommandsSpout());

			BoltDeclarer reader = builder.setBolt("points-reader", new PointsReader("/input.txt"));
			reader.allGrouping("commands-spout", "commands");

			BoltDeclarer processor = builder.setBolt("point-processor", new PointProcessor());
			processor.shuffleGrouping("points-reader");

			BoltDeclarer kmeans = builder.setBolt("k-means-online", new BoltProcessor(), 2);
			// All bolts must get input data
			kmeans.allGrouping("point-processor");
			// All bolts must get signals
			kmeans.allGrouping("signals-spout", "signals");
			// Share jobs between bolts
			kmeans.shuffleGrouping("commands-spout", "commands");
			
			// Only one output point
			BoltDeclarer output = builder.setBolt("output-bolt", new BoltOutput(), 1);
			output.shuffleGrouping("k-means-online");

			Config conf = new Config();
			// conf.registerSerialization(Point.class);
			// conf.put("file", args[0]);
			// conf.setDebug(false);
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
