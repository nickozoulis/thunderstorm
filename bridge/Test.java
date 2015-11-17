package com.constambeys.storm;

import filtering.DataFilter;
import filtering.Point;

public class Test {
	public static void main(String[] args) throws Exception {
		DataFilter f = new DataFilter("x0+x1<2");
		Point p = new Point(2);
		p.components[0] = 1;
		p.components[1] = 1;
		System.out.println(f.run(p));
	}
}