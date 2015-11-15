package fusion.clustering;

import fusion.query.QueryType;

import java.util.HashSet;
import java.util.Set;

/**
 * Created by nickozoulis on 15/11/2015.
 */
public abstract class Query {

    private Set<String> filters;
    private QueryType queryType;

    public Query(QueryType queryType) {
        setFilters(new HashSet<String>());
        setQueryType(queryType);
    }

    public Query(QueryType queryType, Set filters) {
        setFilters(filters);
        setQueryType(queryType);
    }

    public abstract String getQuery();

    @Override
    public abstract boolean equals(Object obj);

    @Override
    public String toString() {
        String s = "";

        for (String str : filters)
            s += " ; " + str;

        return s;
    }

    public Set getFilters() {
        return filters;
    }

    public QueryType getQueryType() {
        return queryType;
    }

    public void setFilters(Set filters) {
        this.filters = filters;
    }

    public void setQueryType(QueryType queryType) {
        this.queryType = queryType;
    }
}
