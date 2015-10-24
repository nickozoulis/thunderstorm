package com.constambeys.storm;

public class Test {
	public static void main(String[] args) throws Exception {
		DataFilter f = new DataFilter("{0}+{1}", "<", "2");
		Point p = new Point(2);
		p.components[0] = 2;
		p.components[1] = 1;
		System.out.println(f.run(p));
	}
}