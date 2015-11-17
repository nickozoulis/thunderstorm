package hbase;

import java.util.ArrayList;

public class KMeans {

	public long id;
	public int k;
	public ArrayList<String> filters;

	public KMeans(long id, int k) {
		this.id = id;
		this.k = k;
		this.filters = new ArrayList<>();
	}

	public void addFilter(String filter) {
		filters.add(filter);
	}

}
