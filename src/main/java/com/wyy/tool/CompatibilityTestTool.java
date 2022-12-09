package com.wyy.tool;

import com.wyy.tool.common.MetricsSystem;
import com.wyy.tool.common.OpCode;
import com.wyy.tool.common.ToolConfig;
import com.wyy.tool.task.AbstractTask;
import com.wyy.tool.task.CreateTask;
import com.wyy.tool.task.DeleteTask;
import com.wyy.tool.task.DiffTask;
import com.wyy.tool.task.LoopTask;
import com.wyy.tool.task.MixTask;
import com.wyy.tool.task.PReadFileTask;
import com.wyy.tool.task.ReadFileTask;
import com.wyy.tool.task.ReadTotalTask;
import lombok.SneakyThrows;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;


public class CompatibilityTestTool {
    final static Logger log = LoggerFactory.getLogger(CompatibilityTestTool.class);

    public static void main(String[] args) throws IOException {
        ToolConfig.getInstance().loadProperties();

        ToolConfig config = ToolConfig.getInstance();
        String Operation = config.getOpName();
        String CONFIG = config.getConfigPath();
        String NNADDR = config.getHost();

        Configuration conf = new Configuration();
//        conf.addResource(new File(CONFIG + "/hdfs-site.xml").toURI().toURL());
        conf.addResource(new File(CONFIG + "/core-site.xml").toURI().toURL());
        conf.setBoolean("fs.hdfs.impl.disable.cache", false);
        conf.setClassLoader(CompatibilityTestTool.class.getClassLoader());
        conf.set("fs.defaultFS", NNADDR);

        excuteTask(Operation, conf);
        System.exit(0);
    }

    public static void excuteTask(String operation, Configuration conf) {
        AbstractTask task;
        if (OpCode.CHECK_STATUS.getOpValue().equals(operation) ||
            OpCode.READ.getOpValue().equals(operation) ||
            OpCode.REST_READ.getOpValue().equals(operation)) {
            task = new ReadFileTask(conf, operation);
        } else if (OpCode.CREATE.getOpValue().equals(operation) ||
            OpCode.REST_CREATE.getOpValue().equals(operation)) {
            task = new CreateTask(conf, operation);
        } else if (OpCode.DELETE.getOpValue().equals(operation)) {
            task = new DeleteTask(conf);
        } else if (OpCode.LOOP.getOpValue().equals(operation)) {
            task = new LoopTask(conf);
        } else if (OpCode.MIX.getOpValue().equals(operation)) {
            task = new MixTask(conf);
        } else {
            System.out.println("not support this op.");
            log.error("wrong operation");
            return;
        }
        task.start();
    }

}
