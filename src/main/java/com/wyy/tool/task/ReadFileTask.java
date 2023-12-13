package com.wyy.tool.task;

import static com.amazonaws.services.s3.internal.Constants.KB;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.GetObjectMetadataRequest;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.codahale.metrics.Timer;
import com.wyy.tool.common.MetricsSystem;
import com.wyy.tool.common.OpCode;
import com.wyy.tool.common.ToolOperator;
import com.wyy.tool.tool.ToolHttpClient;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.http.Header;
import org.apache.http.message.BasicHeader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.wyy.tool.common.ToolConfig;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class ReadFileTask extends AbstractTask {
    final static Logger log = LoggerFactory.getLogger(ReadFileTask.class);
    AtomicBoolean end = new AtomicBoolean(false);

    String operation;

    public ReadFileTask(Configuration conf, String operation) {
        super(conf);
        this.operation = operation;
        qpsMeter = MetricsSystem.meter(this.getClass(), operation, "qps");
        iopsMeter = MetricsSystem.meter(this.getClass(), operation, "iops");
        timer = MetricsSystem.timer(this.getClass(), operation, "latency");
    }

    public void doTask() {
        String userName = ToolConfig.getInstance().getUserName();
        String HostName = ToolConfig.getInstance().getHost();
        String workPath = ToolConfig.getInstance().getWorkPath();
        long durationTime = ToolConfig.getInstance().getReadDurationTime();

        Map<Integer, List<String>> nameLists = new HashMap();
        try {
            URI uri = new URI(HostName + workPath);
            FileSystem fs = FileSystem.get(uri, conf, userName);
            for (int i = 0; i < totalThreads; i++) {
                int currentIndex = i % 20;
                String dst = HostName + workPath + "/TestThread-" + currentIndex + "/";
                FileStatus[] fileStatuses = fs.listStatus(new Path(dst));
                List<String> names = new ArrayList<>();
                for (FileStatus status : fileStatuses) {
                    // rest read need the rest host name instead of default hostname
                    if (OpCode.REST_READ.getOpValue().equals(operation) ||
                        OpCode.RANGE_READ.getOpValue().equals(operation)) {
                        names.add(workPath + "/TestThread-" + i + "/" + status.getPath().getName());
                    } else {
                        names.add(dst + status.getPath().getName());
                    }
                }
                nameLists.put(i, names);
            }
        } catch (Exception e) {
            log.warn("list file exception:", e);
            throw new RuntimeException(e);
        }

        startTime = System.currentTimeMillis();
        MetricsSystem.startReport();
        for (int i = 0; i < totalThreads; i++) {
            Runnable task;
            if (OpCode.READ.getOpValue().equals(operation)) {
                task = new ReadSubTask(userName, nameLists.get(i), conf, latch);
            } else if (OpCode.CHECK_STATUS.getOpValue().equals(operation)) {
                task = new CheckSubTask(userName, nameLists.get(i), conf, latch);
            } else if (OpCode.REST_READ.getOpValue().equals(operation)) {
                task = new RestReadSubTask(userName, nameLists.get(i), conf, latch);
            } else if (OpCode.RANGE_READ.getOpValue().equals(operation)) {
                task = new RangReadSubTask(userName, nameLists.get(i), conf, latch);
            } else {
                return;
            }
            threadPool.execute(task);
        }

        try {
            latch.await(durationTime, TimeUnit.SECONDS);
            end.set(true);
            log.warn("all tasks should meet the end.");
            latch.await();
        } catch (Exception e) {
            log.warn("CheckStatusTask: execute task error,exception:", e);
        } finally {
            threadPool.shutdownNow();
            MetricsSystem.stopReport();
        }
    }

    class ReadSubTask implements Runnable {
        private String user;
        private List<String> nameList;
        private Configuration conf;
        private CountDownLatch latch;

        public ReadSubTask(String user, List<String> nameList, Configuration conf,
                           CountDownLatch latch) {
            this.user = user;
            this.nameList = nameList;
            this.conf = conf;
            this.latch = latch;
        }

        @Override
        public void run() {
            if (nameList.isEmpty()) {
                return;
            }
            FileSystem fs = null;
            try {
                while (true) {
                    URI uri = new URI(nameList.get(0));
                    fs = FileSystem.get(uri, conf, user);
                    for (String path : nameList) {
                        if (end.get()) {
                            return;
                        }
                        Path srcPath = new Path(path);
                        int bufferSize = ToolConfig.getInstance().getReadBufferSize();
                        byte[] b = new byte[bufferSize];
                        int length;
                        FSDataInputStream in = null;
                        try (Timer.Context context = timer.time()) {
                            in = fs.open(srcPath);
                            BufferedInputStream buffer = new BufferedInputStream(in, bufferSize);
                            while ((length = buffer.read(b)) > 0) {
                                iopsMeter.mark(length);
                            }
                        } catch (IOException e) {
                            log.error("read file " + path + " failed! ", e);
                        } finally {
                            if (in != null) {
                                try {
                                    in.close();
                                } catch (IOException ioException) {
                                    log.warn("read file " + path + " close stream failed. ");
                                }
                            }
                            qpsMeter.mark();
                        }
                    }
                }
            } catch (Exception e) {
                log.error("read task exception:", e);
                return;
            } finally {
                if (fs != null) {
                    try {
                        fs.close();
                    } catch (IOException e) {
                        log.warn("fs close error. exception: ", e);
                    }
                }
                latch.countDown();
            }
        }
    }

    class CheckSubTask implements Runnable {
        private String user;
        private List<String> nameList;
        private Configuration conf;
        private CountDownLatch latch;

        public CheckSubTask(String user, List<String> nameList, Configuration conf,
                            CountDownLatch latch) {
            this.user = user;
            this.nameList = nameList;
            this.conf = conf;
            this.latch = latch;
        }

        @Override
        public void run() {
            if (nameList.isEmpty()) {
                return;
            }
            FileSystem fs = null;
            try {
                while (true) {
                    URI uri = new URI(nameList.get(0));
                    fs = FileSystem.get(uri, conf, user);
                    for (String path : nameList) {
                        if (end.get()) {
                            return;
                        }
                        try (Timer.Context context = timer.time()) {
                            ToolOperator.checkFile(path, fs);
                        } finally {
                            qpsMeter.mark();
                        }
                    }
                }
            } catch (Exception e) {
                log.error("read task exception:", e);
            } finally {
                if (fs != null) {
                    try {
                        fs.close();
                    } catch (IOException e) {
                        log.warn("fs close error. exception: ", e);
                    }
                }
                latch.countDown();
            }
        }
    }

    class RestReadSubTask implements Runnable {
        private String user;
        private List<String> nameList;
        private CountDownLatch latch;
        private Configuration conf;

        public RestReadSubTask(String user, List<String> nameList, Configuration conf, CountDownLatch latch) {
            this.user = user;
            this.nameList = nameList;
            this.conf = conf;
            this.latch = latch;
        }

        @Override
        public void run() {
            if (nameList.isEmpty()) {
                return;
            }
            String restHost = ToolConfig.getInstance().getRestHost();
            try {
                while (true) {
                    for (String path : nameList) {
                        if (end.get()) {
                            return;
                        }
                        try (Timer.Context context = timer.time()) {
                            String restUrl = restHost + path;
                            String Authorization = String.format("AWS4-HMAC-SHA256 Credential=%s/3uRmVm7lWfvclsqfpPJN2Ftigi4=", user);
                            Header header = new BasicHeader("Authorization", Authorization);
                            ToolHttpClient.httpGetStream(restUrl, iopsMeter, header);
                        } finally {
                            qpsMeter.mark();
                        }
                    }
                }
            } catch (Exception e) {
                log.error("read task exception:", e);
            } finally {
                latch.countDown();
            }
        }
    }

    class RangReadSubTask implements Runnable {
        private String user;
        private List<String> nameList;
        private CountDownLatch latch;
        private Configuration conf;

        public RangReadSubTask(String user, List<String> nameList, Configuration conf, CountDownLatch latch) {
            this.user = user;
            this.nameList = nameList;
            this.conf = conf;
            this.latch = latch;
        }

        @Override
        public void run() {
            if (nameList.isEmpty()) {
                return;
            }
            String restHost = ToolConfig.getInstance().getRestHost();
            String bucket = ToolConfig.getInstance().getBucketName();
            AmazonS3 s3 = AmazonS3ClientBuilder.standard()
                .withCredentials(new AWSStaticCredentialsProvider(new BasicAWSCredentials(user, "secretKey")))
                .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(restHost, ""))
                .enablePathStyleAccess()
                .build();
            int bufferSize = ToolConfig.getInstance().getReadBufferSize();
            try {
                while (true) {
                    for (String path : nameList) {
                        if (end.get()) {
                            return;
                        }
                        try (Timer.Context context = timer.time()) {
                            GetObjectMetadataRequest request = new GetObjectMetadataRequest(bucket, path);
                            ObjectMetadata meta = s3.getObjectMetadata(request);
                            long length = meta.getContentLength();
                            long totalSize = 0;
                            int i = 0;
                            while (length > totalSize) {
                                GetObjectRequest getObjectRequest = new GetObjectRequest(bucket, path);
                                getObjectRequest.setRange((long) i * bufferSize,
                                    (long) (i + 1) * bufferSize - 1);
                                S3Object object = s3.getObject(getObjectRequest);
                                S3ObjectInputStream objectContent = object.getObjectContent();

                                byte[] buffer = new byte[bufferSize];
                                int readSize;
                                while ((readSize = objectContent.read(buffer)) != -1) {
                                    totalSize += readSize;
                                    iopsMeter.mark(readSize);
                                }
                                objectContent.close();
                                i++;
                            }
                        } finally {
                            qpsMeter.mark();
                        }
                    }
                }
            } catch (Exception e) {
                log.error("read task exception:", e);
            } finally {
                latch.countDown();
            }
        }
    }
}