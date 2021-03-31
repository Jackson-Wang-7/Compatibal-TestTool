package sg.bigo.hdfs;

import lombok.SneakyThrows;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.HadoopIllegalArgumentException;
import org.apache.hadoop.conf.Configuration;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;


public class HdfsTest {
    final static Log log = LogFactory.getLog(HdfsTest.class);
    //操作字
    private static String operation = "none";
    //hadoop namenode 节点ip
    private static String NNADDR = "hdfs://164.90.93.112:8888/";
    //hdfs目录
    private static String HDFSDIR = "/user/litao/testfiles/dir";
    //重试次数
    private static int RETRY_COUNT = 1;
    //配置文件地址
    private static String CONFIG = ".";
    //设定文件数偏移（用于中继上传）
    private static int Offset = 0;
    //上传文件总数
    private static int TOTAL_FILES = 10;
    //上传文件使用线程数
    private static int TOTAL_THREADS = 1;

    private String dst;
    private int FileStartNum;
    private int FileEndNum;
    private Configuration conf;
    private CountDownLatch Latch;

    public HdfsTest(String dst, int fileStartNum, int fileEndNum, Configuration conf, CountDownLatch latch) {
        this.dst = dst;
        FileStartNum = fileStartNum;
        FileEndNum = fileEndNum;
        this.conf = conf;
        Latch = latch;
    }

    @SneakyThrows
    public static void main(String[] args) {
        System.out.println("1:Offset, 2:TOTAL_FILES, 3:TOTAL_THREADS, 4:NNADDR, 5:REMOTEDIR");

        parseInput(args);


        ExecutorService ThreadPool = Executors.newFixedThreadPool(TOTAL_THREADS);
        CountDownLatch Latch = new CountDownLatch(TOTAL_THREADS);
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

        if (operation.equals("readWriteOP")) {
            int SingleFileNum = TOTAL_FILES / TOTAL_THREADS;

            for (int i = 0; i < TOTAL_THREADS; i++) {
                int FileStartNum = i * SingleFileNum + Offset;
                int FileEndNum = (i + 1) * SingleFileNum;
                //hadoop每个文件夹都有文件数量上限，所以此处为每个线程执行的上传新建一个目录
                String dst = NNADDR + HDFSDIR + i + "/";
                ReadWriteTask hlt = new ReadWriteTask(dst, FileStartNum, FileEndNum, conf, Latch);
                ThreadPool.execute(hlt);
            }
        } else if (operation.equals("delete")) {
            String dst = NNADDR + HDFSDIR;
            List<String> pathList = new ArrayList<>();
            pathList.add(dst);
            DeleteTask hlt = new DeleteTask(pathList, conf, Latch);
            ThreadPool.execute(hlt);
        }
        Latch.await();

        System.out.println("################ test report #################");
        Long b = System.currentTimeMillis();
        String et = "end  time  sec :" + b + "\n";
        fileOut.write(et.getBytes());
        String allt = "all time :" + ((b - a) / 1000) + "s" + "\n";
        fileOut.write(allt.getBytes());
        System.out.println("总用时:" + ((b - a) / 1000) + "s");
        System.out.println("##############################################");

        ThreadPool.shutdown();
    }

    public static void parseInput(final String[] args) {
        if (args.length == 0) {
            throw new HadoopIllegalArgumentException("invalid param.");
        }

        for (int i=0;i<args.length;i++) {
            if (args[i].equals("-op")) {
                operation = args[++i];
            } else if (args[i].equals("-offset")) {
                Offset = Integer.valueOf(args[++i]);
            } else if (args[i].equals("-total")) {
                TOTAL_FILES = Integer.valueOf(args[++i]);
            } else if (args[i].equals("-addr")) {
                NNADDR = args[++i];
            } else if (args[i].equals("-path")) {
                HDFSDIR = args[++i];
            } else if (args[i].equals("-thread")) {
                TOTAL_THREADS = Integer.valueOf(args[++i]);
            }

        }
    }



}
