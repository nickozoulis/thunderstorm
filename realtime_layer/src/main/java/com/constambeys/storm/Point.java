package com.constambeys.storm;

import java.io.Serializable;

public class Point implements Serializable {

	double[] components;

	public Point(int dimension) {
		components = new double[dimension];
	}

	public Point(String[] values) {
		components = new double[values.length];

		for (int i = 0; i < values.length; i++) {
			components[i] = Double.parseDouble(values[i]);
		}
	}

	public void add(Point x) {
		for (int i = 0; i < x.components.length; i++) {
			components[i] = components[i] + x.components[i];
		}

	}

	public void multibly(int x) {
		for (int i = 0; i < components.length; i++) {
			components[i] = components[i] * x;
		}
	}

	public void divide(int x) {
		for (int i = 0; i < components.length; i++) {
			components[i] = components[i] / x;
		}
	}

	public int getDimesion() {
		return components.length;
	}

	public String print() {
		StringBuilder sb = new StringBuilder();

		sb.append("(");

		for (int i = 0; i < components.length - 1; i++) {
			sb.append(components[i]);
			sb.append(",");
		}
		sb.append(components[components.length - 1]);
		sb.append(")");

		return sb.toString();
	}

	public static double distance(Point a, Point b) {

		double distance = 0;

		for (int i = 0; i < a.components.length; i++) {
			distance = distance + (a.components[i] - b.components[i]) * (a.components[i] - b.components[i]);
		}

		return Math.sqrt(distance);
	}

}
