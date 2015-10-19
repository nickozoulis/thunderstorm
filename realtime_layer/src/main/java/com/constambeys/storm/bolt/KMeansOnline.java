package com.constambeys.storm.bolt;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

public class KMeansOnline implements IRichBolt {

	final int CONSTANT = 3;

	class D implements Comparable<D> {
		double dist;
		int index;

		public int compareTo(D o) {
			return Double.compare(dist, o.dist);
		}
	}

	class Ds {
		int size = 0;
		final int max = CONSTANT - 1;
		D[] d = new D[max];
	}

	Integer id;
	String name;
	boolean initilization = true;
	int counters[];
	double meansX[];
	double meansY[];
	int k;

	private OutputCollector collector;

	public KMeansOnline(int k) {
		this.k = k;
	}

	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		counters = new int[k * CONSTANT];
		meansX = new double[k * CONSTANT];
		meansY = new double[k * CONSTANT];
		this.collector = collector;
		this.name = context.getThisComponentId();
		this.id = context.getThisTaskId();
		System.out.println("bolt: " + this.name + " id: " + this.id + " prepared #####################");
	}

	public void execute(Tuple input) {
		/**
		 * Handle signal to clear cache
		 */

		if (input.getSourceStreamId().equals("signals")) {
			if ("refresh".equals(input.getStringByField("action"))) {
				// counters.clear();
				return;
			} else if ("print".equals(input.getStringByField("action"))) {
				print();
			}
			return;
		}

		int x = input.getInteger(0);
		int y = input.getInteger(1);

		if (initilization) {
			boolean found = false;
			for (int i = 0; i < counters.length; i++) {
				if (counters[i] == 0) {
					meansX[i] = x;
					meansY[i] = y;
					counters[i] = 1;
					found = true;
					// Set the tuple as Acknowledge
					collector.ack(input);
					break;
				}
			}
			if (found == false) {
				initilization = false;
				execute(input);
			}
		} else {
			int index = 0;
			double min = distance(meansX[0], meansY[0], x, y);

			for (int i = 1; i < meansX.length; i++) {
				double dist = distance(meansX[i], meansY[i], x, y);
				if (dist < min) {
					min = dist;
					index = i;
				}
			}

			counters[index]++;
			meansX[index] = meansX[index] + 1.0 / counters[index] * (x - meansX[index]);
			meansY[index] = meansY[index] + 1.0 / counters[index] * (y - meansY[index]);
			// Set the tuple as Acknowledge
			collector.ack(input);
		}

	}

	private void print() {
		boolean[] used = new boolean[k * CONSTANT];

		for (int u1 = 0; u1 < used.length; u1++) {
			if (used[u1] == false) {
				used[u1] = true;

				Ds ds = new Ds();

				for (int u2 = 0; u2 < used.length; u2++) {

					if (used[u2] == false) {
						insertDistance(meansX[u1], meansY[u1], meansX[u2], meansY[u2], ds, u2);
					}
				}

				double xx = meansX[u1], yy = meansX[u1];
				for (int i = 0; i < ds.d.length; i++) {
					xx += meansX[ds.d[i].index];
					yy += meansY[ds.d[i].index];
					used[ds.d[i].index] = true;
				}
				System.out.print(String.format("(%f,%f) ", xx / CONSTANT, yy / CONSTANT));
			}

		}

		// for (int i = 0; i < meansX.length; i++) {
		// System.out.print(String.format("(%f,%f) ", meansX[i],
		// meansY[i]));
		// }
		System.out.println();
	}

	private void insertDistance(double x1, double y1, double x2, double y2, Ds ds, int index) {
		double x = distance(x1, y1, x2, y2);
		if (ds.size < ds.max) {
			ds.d[ds.size] = new D();
			ds.d[ds.size].index = index;
			ds.d[ds.size].dist = x;
			ds.size++;
		} else {
			Arrays.sort(ds.d);
			if (ds.d[ds.size - 1].dist > x) {
				ds.d[ds.size - 1].index = index;
				ds.d[ds.size - 1].dist = x;
			}
		}
	}

	private double distance(double x1, double y1, double x2, double y2) {
		return Math.sqrt((x1 - x2) * (x1 - x2) + (y1 - y2) * (y1 - y2));
	}

	public void cleanup() {

	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("word"));

	}

	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

}
