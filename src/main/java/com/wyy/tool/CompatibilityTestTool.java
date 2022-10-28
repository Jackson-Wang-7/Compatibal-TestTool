package com.wyy.tool;

import com.wyy.tool.common.MetricsSystem;
import com.wyy.tool.common.ToolConfig;
import com.wyy.tool.task.AbstractTask;
import com.wyy.tool.task.CheckStatusTask;
import com.wyy.tool.task.CreateTask;
import com.wyy.tool.task.DeleteTask;
import com.wyy.tool.task.DiffTask;
import com.wyy.tool.task.LoopTask;
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
    }

    public static void excuteTask(String Operation, Configuration conf) {
        AbstractTask task;
        switch (Operation) {
            case "check":
                task = new CheckStatusTask(conf);
                break;

            case "create":
                task = new CreateTask(conf);
                break;

            case "delete":
                task = new DeleteTask(conf);
                break;

            case "read":
                task = new ReadFileTask(conf);
                break;

//            case "readTotal":
//                ReadTotalTask.doTask(conf);
//                break;
//
//            case "pread":
//                PReadFileTask.doTask(conf);
//                break;
//
//            case "rest_read":
//                RestReadFileTask.doTask(conf);
//                break;

//            case "diff":
//                DiffTask.doTask(conf);
//                break;
//
            case "loop":
                task = new LoopTask(conf);
                break;
//
//            case "mix":

            default:
                System.out.println("not support this op.");
                log.error("wrong operation");
                return;
        }
        task.start();
    }

}
