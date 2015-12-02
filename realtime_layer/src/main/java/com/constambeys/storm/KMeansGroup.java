package com.constambeys.storm;

import javax.script.ScriptException;

import filtering.Point;

public class KMeansGroup {

	boolean first_update;
	KMeansOnline k1;
	KMeansOnline k2;

	public KMeansGroup(KMeansOnline k) {
		first_update = true;
		k1 = k;
		k2 = new KMeansOnline(k.id, k.k);
		k2.filters = k.filters;
	}

	public int getID() {
		return k1.id;
	}

	public int getK() {
		return k1.k;
	}

	public KMeansOnline getPrint() {
		return k1;
	}

	public void update(Point[] point) {

		if (first_update) {
			first_update = false;
			k1.update(point);
			k2.clear();
		} else {
			first_update = true;
			KMeansOnline temp = k1;
			k1 = k2;
			k2 = temp;
			k1.update(point);
			k2.clear();
		}

	}

	public void run(Point point) throws ScriptException {
		k1.run(point);
		k2.run(point);
	}

}
