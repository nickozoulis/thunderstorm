package clustering;

import net.sf.javaml.core.Dataset;

/**
 * Created by nickozoulis on 15/11/2015.
 */
public class RemoteKMeans extends KMeansBase {

    public RemoteKMeans(KMeansQuery query, Dataset dataset) {
        super(query, dataset);
    }

    @Override
    public Dataset[] cluster() {
        return new Dataset[0];
    }
}
