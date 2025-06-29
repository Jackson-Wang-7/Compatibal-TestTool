package com.wyy.tool.task;

import com.amazonaws.ClientConfiguration;
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
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.core.async.AsyncResponseTransformer;
import software.amazon.awssdk.http.crt.AwsCrtAsyncHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.S3Object;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
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
        if (OpCode.RANGE_READ.getOpValue().equals(operation) ||
            OpCode.REST_READ.getOpValue().equals(operation)) {
            listFromS3(workPath, nameLists);
        } else {
            listFromFS(userName, HostName, workPath, nameLists);
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
            latch.await(10, TimeUnit.SECONDS);
        } catch (Exception e) {
            log.warn("CheckStatusTask: execute task error,exception:", e);
        } finally {
            threadPool.shutdownNow();
            MetricsSystem.stopReport();
        }
    }

    private void listFromS3(String workPath, Map<Integer, List<String>> nameLists) {
        String restHost = ToolConfig.getInstance().getRestHost();
        String bucket = ToolConfig.getInstance().getBucketName();
        String ak = ToolConfig.getInstance().getAccessKey();
        String sk = ToolConfig.getInstance().getSecretKey();
        String region = ToolConfig.getInstance().getRegion();
        boolean listThread = ToolConfig.getInstance().isListThreadPrefix();
        URI uri;
        try {
            uri = new URI(restHost);
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
        // Singleton: Use s3AsyncClient for all requests.
        S3AsyncClient s3AsyncClient = S3AsyncClient.builder()
            .httpClientBuilder(AwsCrtAsyncHttpClient
                .builder()
                .connectionTimeout(Duration.ofSeconds(3))
                .maxConcurrency(100))
            .credentialsProvider(() -> AwsBasicCredentials.create(ak, sk))
            .endpointOverride(uri)
            .region(Region.of(region))
            .forcePathStyle(true)
            .build();

        for (int i=0;i<totalThreads;i++) {
            String prefix;
            if (listThread) {
                int currentIndex = i % 20;
                prefix = workPath + "/TestThread-" + currentIndex + "/";
            } else {
                prefix = workPath;
            }
            ListObjectsV2Request req =
                ListObjectsV2Request.builder().bucket(bucket).prefix(prefix).build();
            ListObjectsV2Response result = s3AsyncClient.listObjectsV2(req).join();
            List<String> names = new ArrayList<>();
            for (S3Object s3Object : result.contents()) {
                names.add(s3Object.key());
            }
            nameLists.put(i, names);
        }
    }

    private void listFromFS(String userName, String HostName, String workPath,
                           Map<Integer, List<String>> nameLists) {
        try {
            URI uri = new URI(HostName + workPath);
            FileSystem fs = FileSystem.get(uri, conf, userName);
            boolean listThread = ToolConfig.getInstance().isListThreadPrefix();
            for (int i = 0; i < totalThreads; i++) {
                String dst;
                if (listThread) {
                    int currentIndex = i % 20;
                    dst = HostName + workPath + "/TestThread-" + currentIndex + "/";
                } else {
                    dst = workPath;
                }
                FileStatus[] fileStatuses = fs.listStatus(new Path(dst));
                List<String> names = new ArrayList<>();
                for (FileStatus status : fileStatuses) {
                    names.add(dst + status.getPath().getName());
                }
                nameLists.put(i, names);
            }
        } catch (Exception e) {
            log.warn("list file exception:", e);
            throw new RuntimeException(e);
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
                        rangeReadPath(fs, path);
//                        readPath(fs, path);
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

        private void rangeReadPath(FileSystem fs, String path) {
            Path srcPath = new Path(path);
            int bufferSize = ToolConfig.getInstance().getReadBufferSize();
            int rangeSize = (int) ToolConfig.getInstance().getRangeSize();
            bufferSize = Math.min(bufferSize, rangeSize);
            byte[] b = new byte[bufferSize];
            int length = 0;
            FSDataInputStream in = null;
            long start = 0;
            while (length >= 0) {
                if (end.get()) {
                    return;
                }
                try (Timer.Context context = timer.time()) {
                    in = fs.open(srcPath);
                    in.seek(start);
                    BufferedInputStream buffer = new BufferedInputStream(in, bufferSize);
                    int currentRead = 0;
                    while ((currentRead < rangeSize) &&
                        (length = buffer.read(b, 0, rangeSize)) > 0) {
                        iopsMeter.mark(length);
                        currentRead += length;
                    }
                    start += currentRead;
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

        private void readPath(FileSystem fs, String path) {
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
            String bucket = ToolConfig.getInstance().getBucketName();
            try {
                while (true) {
                    for (String path : nameList) {
                        if (end.get()) {
                            return;
                        }
                        if (path.endsWith("/")) {
                            continue;
                        }
                        try (Timer.Context context = timer.time()) {
                            String restUrl = restHost + bucket + "/" + path;
                            String Authorization = String.format("AWS4-HMAC-SHA256 Credential=%s/3uRmVm7lWfvclsqfpPJN2Ftigi4=", user);
                            Header header = new BasicHeader("Authorization", Authorization);
                            ToolHttpClient.getInstance().httpGetStream(restUrl, iopsMeter, header);
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
            try {
                if (nameList.isEmpty()) {
                    return;
                }
                String restHost = ToolConfig.getInstance().getRestHost();
                String bucket = ToolConfig.getInstance().getBucketName();
                long rangeSize = ToolConfig.getInstance().getRangeSize();
                boolean keepAlive = ToolConfig.getInstance().isTcpKeepAlive();
                ClientConfiguration clientConfiguration =
                    new ClientConfiguration().withSocketTimeout(300 * 1000)
                        .withTcpKeepAlive(keepAlive);

                String ak = ToolConfig.getInstance().getAccessKey();
                String sk = ToolConfig.getInstance().getSecretKey();
                String region = ToolConfig.getInstance().getRegion();
                URI uri = new URI(restHost);
                // Singleton: Use s3AsyncClient for all requests.
                S3AsyncClient s3AsyncClient = S3AsyncClient.builder()
                    .httpClientBuilder(AwsCrtAsyncHttpClient
                        .builder()
                        .connectionTimeout(Duration.ofSeconds(300))
                        .maxConcurrency(100))
                    .credentialsProvider(() -> AwsBasicCredentials.create(ak, sk))
                    .endpointOverride(uri)
                    .region(Region.of(region))
                    .forcePathStyle(true)
                    .build();
                int bufferSize = ToolConfig.getInstance().getReadBufferSize();
                while (true) {
                    for (String path : nameList) {
                        if (path.endsWith("/")) {
                            continue;
                        }
                        if (end.get()) {
                            return;
                        }
                        rangeReadS3(bucket, s3AsyncClient, rangeSize, bufferSize, path, keepAlive);
                    }
                }
            } catch (Exception e) {
                log.error("read task exception:", e);
            } finally {
                latch.countDown();
            }
        }

        private void rangeReadS3(String bucket, S3AsyncClient s3, long rangeSize, int bufferSize,
                                 String path, boolean keepAlive)
            throws IOException {
            HeadObjectRequest request = HeadObjectRequest.builder().bucket(bucket).key(path).build();
            HeadObjectResponse meta = s3.headObject(request).join();
            long length = meta.contentLength();
            long totalSize = 0;
            int partNumber = (int) (length / rangeSize);
            Random rand = new Random();
            int randomNum = rand.nextInt(partNumber);
            while (length > totalSize) {
                if (end.get()) {
                    return;
                }
                try (Timer.Context context = timer.time()) {
                    GetObjectRequest getObjectRequest = GetObjectRequest.builder()
                        .bucket(bucket)
                        .key(path)
                        .range("bytes=" + randomNum * rangeSize + "-" + ((randomNum + 1) * rangeSize - 1)).build();
                    InputStream objectContent = s3.getObject(getObjectRequest,
                        AsyncResponseTransformer.toBlockingInputStream()).join();

                    byte[] buffer = new byte[bufferSize];
                    int readSize;
                    while ((readSize = objectContent.read(buffer)) != -1) {
                        totalSize += readSize;
                        iopsMeter.mark(readSize);
                    }
                    objectContent.close();
                } catch (Exception e) {
                    log.error("read file {} exception:{}", path, e.getMessage());
                } finally {
                    qpsMeter.mark();
                }
            }
        }
    }
}
