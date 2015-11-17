package clustering;

import java.util.HashSet;
import java.util.Set;

/**
 * Created by nickozoulis on 15/11/2015.
 */
public abstract class Query {

    private Set<String> filters;
    private QueryType queryType;
    private long id;

    public Query(QueryType queryType) {
        setFilters(new HashSet<String>());
        setQueryType(queryType);
        setId(-1);
    }

    public Query(QueryType queryType, long id) {
        setFilters(new HashSet<String>());
        setQueryType(queryType);
        setId(id);
    }

    public Query(QueryType queryType, Set filters) {
        setFilters(filters);
        setQueryType(queryType);
        setId(-1);
    }

    public Query(QueryType queryType, long id, Set filters) {
        setFilters(filters);
        setQueryType(queryType);
        setId(id);
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

    public Set<String> getFilters() {
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

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }
}
