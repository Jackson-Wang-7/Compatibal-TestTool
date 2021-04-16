package sg.bigo.hdfs.task;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import sg.bigo.hdfs.HdfsTest;
import sg.bigo.hdfs.common.HDFSConfig;

public class MixTask {
    final static Log log = LogFactory.getLog(MixTask.class);

    public static void doTask(Configuration conf) {
        String ops = HDFSConfig.getInstance().getMixOps();
        int loopCount = HDFSConfig.getInstance().getLoopCount();
        int count = 1;
        while (count <= loopCount) {
            String[] operations = ops.split(",");
            for (String op: operations) {
                HdfsTest.excuteTask(op, conf);
            }
            log.info("MixTask: finished a mix task , current loop:" + count);
            count ++;
        }
    }
}
