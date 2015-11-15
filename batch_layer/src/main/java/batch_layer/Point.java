package batch_layer;

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

	public Point(double[] point) {
		components = point;
	}

	public void add(Point x) {
		for (int i = 0; i < x.components.length; i++) {
			components[i] = components[i] + x.components[i];
		}

	}

	public void multiply(int x) {
		for (int i = 0; i < components.length; i++) {
			components[i] = components[i] * x;
		}
	}

	public void divide(int x) {
		for (int i = 0; i < components.length; i++) {
			components[i] = components[i] / x;
		}
	}

	public int getDimension() {
		return components.length;
	}

	public static double distance(Point a, Point b) {

		double distance = 0;

		for (int i = 0; i < a.components.length; i++) {
			distance = distance + (a.components[i] - b.components[i]) * (a.components[i] - b.components[i]);
		}

		return Math.sqrt(distance);
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();

		for (int i = 0; i < components.length - 1; i++) {
			sb.append(String.format("%.2f", components[i]));
			sb.append(",");
		}
		sb.append(components[components.length - 1]);

		return sb.toString();
	}

}