package clustering;

import net.sf.javaml.clustering.Clusterer;
import net.sf.javaml.clustering.KMeans;
import net.sf.javaml.core.Dataset;
import org.apache.log4j.Logger;

/**
 * Created by nickozoulis on 15/11/2015.
 */
public class LocalKMeans extends KMeansBase {

    private static final Logger logger = Logger.getLogger(LocalKMeans.class);
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
        long startTime = 0, endTime = 0;

        startTime = System.currentTimeMillis();
        Dataset[] d = km.cluster(getDataset());
        endTime = System.currentTimeMillis();

        logger.info(">> local kmeans [" + getQuery() + "] [duration: " + Math.abs(endTime-startTime) + " ms] <<");

        return d;
    }

}
