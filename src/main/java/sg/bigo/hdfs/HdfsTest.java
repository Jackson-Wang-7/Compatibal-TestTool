package sg.bigo.hdfs;

import lombok.SneakyThrows;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import sg.bigo.hdfs.common.HDFSConfig;
import sg.bigo.hdfs.task.CheckStatusTask;
import sg.bigo.hdfs.task.CreateTask;
import sg.bigo.hdfs.task.DeleteTask;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;


public class HdfsTest {
    final static Log log = LogFactory.getLog(HdfsTest.class);

    @SneakyThrows
    public static void main(String[] args) {
        System.out.println("1:Offset, 2:TOTAL_FILES, 3:TOTAL_THREADS, 4:NNADDR, 5:REMOTEDIR");

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
        conf.setClassLoader(HdfsTest.class.getClassLoader());
        conf.set("fs.defaultFS", NNADDR);


        File file = new File("record.txt");
        FileOutputStream fileOut = new FileOutputStream(file, true);
        Long a = System.currentTimeMillis();
        String st = "start time sec :" + a + "\n";
        fileOut.write(st.getBytes());

        switch (Operation) {
            case "check_status":
                CheckStatusTask.doTask(conf);
                break;

            case "crete":
                CreateTask.doTask(conf);
                break;

            case "delete":
                DeleteTask.doTask(conf);
                break;

            case "mix":
                System.out.println("not support this op.");
                log.warn("not support this op.");
                break;

            default:
                log.error("wrong operation");
                throw new IOException("wrong operation");
        }

        System.out.println("################ test report #################");
        Long b = System.currentTimeMillis();
        String et = "end  time  sec :" + b + "\n";
        fileOut.write(et.getBytes());
        String allt = "all time :" + ((b - a) / 1000) + "s" + "\n";
        fileOut.write(allt.getBytes());
        System.out.println("总用时:" + ((b - a) / 1000) + "s");
        System.out.println("##############################################");

    }
}
