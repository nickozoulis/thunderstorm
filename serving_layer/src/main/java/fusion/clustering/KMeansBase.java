package fusion.clustering;

import fusion.query.KMeansQuery;
import net.sf.javaml.core.Dataset;

/**
 * Created by nickozoulis on 15/11/2015.
 */
public abstract class KMeansBase {

    private KMeansQuery query;
    private Dataset dataset;

    public KMeansBase(KMeansQuery query, Dataset dataset) {
        setQuery(query);
        setDataset(dataset);
    }

    public abstract Dataset[] cluster();

    public Dataset getDataset() {
        return dataset;
    }

    public void setDataset(Dataset dataset) {
        this.dataset = dataset;
    }

    public KMeansQuery getQuery() {
        return query;
    }

    public void setQuery(KMeansQuery query) {
        this.query = query;
    }

}
