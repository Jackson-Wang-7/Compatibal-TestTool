package com.wyy.hdfs;

import com.wyy.hdfs.common.HDFSConfig;
import com.wyy.hdfs.task.CheckStatusTask;
import com.wyy.hdfs.task.CreateTask;
import com.wyy.hdfs.task.DeleteTask;
import com.wyy.hdfs.task.DiffTask;
import com.wyy.hdfs.task.MixTask;
import com.wyy.hdfs.task.PReadFileTask;
import com.wyy.hdfs.task.ReadFileTask;
import com.wyy.hdfs.task.ReadTotalTask;
import lombok.SneakyThrows;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;


public class HdfsTest {
    final static Logger log = LoggerFactory.getLogger(HdfsTest.class);

    @SneakyThrows
    public static void main(String[] args) {
        HDFSConfig.getInstance().loadProperties();

        HDFSConfig config = HDFSConfig.getInstance();
        String Operation = config.getOpName();
        String CONFIG = config.getConfigPath();
        String NNADDR = config.getHost();

        Configuration conf = new Configuration();
        conf.addResource(new File(CONFIG + "/hdfs-site.xml").toURI().toURL());
        conf.addResource(new File(CONFIG + "/core-site.xml").toURI().toURL());
        conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
        conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
        conf.setBoolean("fs.hdfs.impl.disable.cache", false);
        conf.setClassLoader(HdfsTest.class.getClassLoader());
        conf.set("fs.defaultFS", NNADDR);

        File file = new File("record.txt");
        FileOutputStream fileOut = new FileOutputStream(file, true);
        Long a = System.currentTimeMillis();
        String st = "execute op " + Operation + "start time:" + a + "\n";
        fileOut.write(st.getBytes());

        excuteTask(Operation, conf);

        System.out.println("################ test report #################");
        Long b = System.currentTimeMillis();
        String et = "end  time :" + b + "\n";
        fileOut.write(et.getBytes());
        String allt = "all time :" + (b - a) + "ms" + "\n";
        fileOut.write(allt.getBytes());
        System.out.println("总用时:" + (b - a) + "ms");
        System.out.println("##############################################");

        System.exit(0);
    }

    public static void excuteTask(String Operation, Configuration conf) {
        switch (Operation) {
            case "check":
                CheckStatusTask.doTask(conf);
                break;

            case "create":
                CreateTask.doTask(conf);
                break;

            case "delete":
                DeleteTask.doTask(conf);
                break;

            case "read":
                ReadFileTask.doTask(conf);
                break;

            case "readTotal":
                ReadTotalTask.doTask(conf);
                break;

            case "pread":
                PReadFileTask.doTask(conf);
                break;
//
//            case "rest_read":
//                RestReadFileTask.doTask(conf);
//                break;

            case "diff":
                DiffTask.doTask(conf);
                break;

            case "mix":
                MixTask.doTask(conf);
                break;

            default:
                System.out.println("not support this op.");
                log.error("wrong operation");
                break;
        }
    }

}
