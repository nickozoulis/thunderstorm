package clustering;

import net.sf.javaml.clustering.Clusterer;
import net.sf.javaml.clustering.KMeans;
import net.sf.javaml.core.Dataset;

/**
 * Created by nickozoulis on 15/11/2015.
 */
public class LocalKMeans extends KMeansBase {

    private Clusterer km;

    public LocalKMeans(KMeansQuery query, Dataset dataset) {
        super(query, dataset);
        km = new KMeans(getQuery().getK());
    }

    /**
     * Returns an array of Datasets, each Dataset represents a cluster.
     */
    @Override
    public Dataset[] cluster() {
        return km.cluster(getDataset());
    }

}
