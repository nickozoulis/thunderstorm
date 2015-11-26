package experiment;

import shell.HBaseUtils;

import java.util.List;

/**
 * Created by nickozoulis on 26/11/2015.
 */
public class ExperimentDriver {


    public ExperimentDriver(List<String> queryList) {
        HBaseUtils.setHBaseConfig();



    }
}
