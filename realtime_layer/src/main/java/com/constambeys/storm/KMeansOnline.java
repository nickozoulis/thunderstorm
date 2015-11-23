package com.constambeys.storm;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;

import javax.script.ScriptException;

import filtering.DataFilter;
import filtering.Point;

public class KMeansOnline implements Serializable {

	final int CONSTANT = 1;

	private class D implements Comparable<D> {
		double dist;
		int index;

		public int compareTo(D o) {
			return Double.compare(dist, o.dist);
		}
	}

	private class Ds {
		int size = 0;
		final int max = CONSTANT - 1;
		D[] d = new D[max];
	}

	boolean initilization;
	int counters[];
	Point means[]; // k points
	public int k; // number of clusters
	public int id;

	// List of filters
	ArrayList<DataFilter> filters = new ArrayList<DataFilter>(0);

	public KMeansOnline(int id, int k) {
		this.k = k;
		this.id = id;
		counters = new int[k * CONSTANT];
		means = new Point[k * CONSTANT];
		initilization = true;
	}

	public void add(DataFilter f) {
		filters.add(f);
	}

	public void clear() {
		counters = new int[k * CONSTANT];
		means = new Point[k * CONSTANT];
		initilization = true;
	}

	public void setStart(Point start[]) {
		means = start;
	}

	public void run(Point point) throws ScriptException {

		if (!checkFilters(point)) {
			return;
		}

		if (initilization) {
			boolean found = false;
			for (int i = 0; i < counters.length - 1; i++) {
				if (counters[i] == 0) {
					means[i] = new Point(point.getDimension());
					means[i].add(point); // Initialisation do not use means[i] = point
					counters[i] = 1;
					found = true;
					break;
				}
			}
			if (found == false) {
				// Check last point
				int i = counters.length - 1;
				if (counters[i] == 0) {
					means[i] = new Point(point.getDimension());
					means[i].add(point); // Initialisation do not use means[i] = point
					counters[i] = 1;
					found = true;
				}
				initilization = false;
			}
		} else {
			int index = 0;
			double min = Point.distance(means[0], point);

			for (int i = 1; i < means.length; i++) {
				double dist = Point.distance(means[i], point);
				if (dist < min) {
					min = dist;
					index = i;
				}
			}

			counters[index]++;
			// meansX[index] = meansX[index] + 1.0 / counters[index] * (x -
			// meansX[index]);
			// meansY[index] = meansY[index] + 1.0 / counters[index] * (y -
			// meansY[index]);
			means[index].multiply(counters[index] - 1);
			means[index].add(point);
			means[index].divide(counters[index]);
		}
	}

	public Point[] result() {

		if (initilization) {
			return null;
		} else {
			int current = 0;
			Point result[] = new Point[k];

			boolean[] used = new boolean[k * CONSTANT];

			for (int u1 = 0; u1 < used.length; u1++) {
				if (used[u1] == false) {
					used[u1] = true;

					Ds ds = new Ds();

					for (int u2 = 0; u2 < used.length; u2++) {

						if (used[u2] == false) {
							insertDistance(means[u1], means[u2], ds, u2);
						}
					}

					Point s = new Point(means[u1].getDimension());
					s.add(means[u1]);
					for (int i = 0; i < ds.d.length; i++) {
						s.add(means[ds.d[i].index]);
						used[ds.d[i].index] = true;
					}
					s.divide(CONSTANT);

					result[current] = s;
					current++;
				}

			}
			return result;
		}

	}

	private boolean checkFilters(Point point) throws ScriptException {

		for (DataFilter f : filters) {
			if (f.run(point) == false) {
				return false;
			}
		}

		return true;

	}

	private void insertDistance(Point x, Point y, Ds ds, int index) {
		if (ds.size > 0) {
			double z = Point.distance(x, y);
			if (ds.size < ds.max) {
				ds.d[ds.size] = new D();
				ds.d[ds.size].index = index;
				ds.d[ds.size].dist = z;
				ds.size++;
			} else {
				Arrays.sort(ds.d);
				if (ds.d[ds.size - 1].dist > z) {
					ds.d[ds.size - 1].index = index;
					ds.d[ds.size - 1].dist = z;
				}
			}
		}
	}

}
