package com.wyy.tool.task;

import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.wyy.tool.BenchmarkTool;
import com.wyy.tool.common.ToolConfig;

public class LoopTask {
    final static Logger log = LoggerFactory.getLogger(LoopTask.class);

    public static void doTask(Configuration conf) {
        String ops = ToolConfig.getInstance().getMixOps();
        int loopCount = ToolConfig.getInstance().getLoopCount();
        int count = 1;
        while (count <= loopCount) {
            String[] operations = ops.split(",");
            for (String op: operations) {
                BenchmarkTool.excuteTask(op, conf);
            }
            log.info("MixTask: finished a mix task , current loop:" + count);
            count ++;
        }
    }
}
