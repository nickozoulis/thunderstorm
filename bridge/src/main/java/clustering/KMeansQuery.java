package clustering;

import java.util.Set;

/**
 * Created by nickozoulis on 15/11/2015.
 */
public class KMeansQuery extends Query {

    private int k;

    public KMeansQuery(int k) {
        super(QueryType.KMEANS);
        setK(k);
    }

    public KMeansQuery(long id, int k) {
        super(QueryType.KMEANS, id);
        setK(k);
    }

    public KMeansQuery(int k, Set filters) {
        super(QueryType.CONSTRAINED_KMEANS, filters);
        setK(k);
    }

    public KMeansQuery(long id, int k, Set filters) {
        super(QueryType.CONSTRAINED_KMEANS, id, filters);
        setK(k);
    }

    @Override
    public String getQuery() {
        return toString();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof KMeansQuery) {
            KMeansQuery q = (KMeansQuery)obj;

            if (this.getQueryType() == q.getQueryType() && this.getQueryType() == QueryType.KMEANS)
                if (this.getK() == q.getK())
                    return true;

            if (this.getQueryType() == q.getQueryType() && this.getQueryType() == QueryType.CONSTRAINED_KMEANS)
                if (this.getK() == q.getK())
                    if (this.getFilters().equals(q.getFilters()))
                        return true;
        }

        return false;
    }

    @Override
    public String toString() {
        String s = "";

        s += "kmeans " + getK();
        s += super.toString();

        return s;
    }

    public int getK() {
        return k;
    }

    public void setK(int k) {
        this.k = k;
    }

}
