package com.wyy.tool.task;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.CompleteMultipartUploadRequest;
import com.amazonaws.services.s3.model.CompleteMultipartUploadResult;
import com.amazonaws.services.s3.model.CopyPartRequest;
import com.amazonaws.services.s3.model.CopyPartResult;
import com.amazonaws.services.s3.model.DeleteObjectsRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadResult;
import com.amazonaws.services.s3.model.PartETag;
import com.amazonaws.services.s3.model.UploadPartRequest;
import com.amazonaws.services.s3.model.UploadPartResult;
import com.codahale.metrics.Snapshot;
import com.codahale.metrics.Timer;
import com.wyy.tool.common.MetricsSystem;
import com.wyy.tool.common.OpCode;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.wyy.tool.common.ToolConfig;

import java.io.File;
import java.io.FileOutputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

import static com.wyy.tool.common.ToolOperator.*;

public class CreateTask extends AbstractTask {
    final static Logger log = LoggerFactory.getLogger(CreateTask.class);

    Timer uploadPartTimer;
    Timer completePartTimer;

    String operation;
    public CreateTask(Configuration conf, String operation) {
        super(conf);
        this.operation = operation;
        qpsMeter = MetricsSystem.meter(this.getClass(), "request", "qps");
        iopsMeter = MetricsSystem.meter(this.getClass(), "request", "iops");
        timer = MetricsSystem.timer(this.getClass(), "request", "latency");
        uploadPartTimer = MetricsSystem.timer(this.getClass(), "uploadPart", "latency");
        completePartTimer = MetricsSystem.timer(this.getClass(), "completePart", "latency");
    }

    public void doTask() {
        int totalFiles = ToolConfig.getInstance().getTotalFiles();
        int totalThreads = ToolConfig.getInstance().getTotalThreads();
        String nameType = ToolConfig.getInstance().getCreateFileNameType();
        String userName = ToolConfig.getInstance().getUserName();
        String HostName = ToolConfig.getInstance().getHost();
        String workPath = ToolConfig.getInstance().getWorkPath();
        String bucket = ToolConfig.getInstance().getBucketName();
        String filePrefix = ToolConfig.getInstance().getCreateFilePrefix();

        //prepare test file
        long fileSize = ToolConfig.getInstance().getCreateSizePerFile();
        String src = ToolConfig.getInstance().getCreateFilePath();
        if (fileSize >= 0) {
            prepareTestFile(fileSize, src);
        }

        int fileNumPerThread = totalFiles / totalThreads;
        //prepare name list
        Map<Integer, List<String>> nameLists =
            prepareNamelists(totalThreads, nameType, filePrefix, fileNumPerThread);

        startTime = System.currentTimeMillis();
        MetricsSystem.startReport();
        // submit task
        for (int i = 0; i < totalThreads; i++) {
            Runnable task;
            if (OpCode.CREATE.getOpValue().equals(operation)) {
                String dst = HostName + workPath + "/TestThread-" + i + "/";
                task = new CreateSubTask(src, dst, userName, nameLists.get(i), fileSize, conf, latch);
            } else if (OpCode.S3_CREATE.getOpValue().equals(operation)) {
                String dst = workPath + "/TestThread-" + i + "/";
                task = new RestCreateSubTask(src, bucket, dst, userName, nameLists.get(i), fileSize,
                    conf, latch);
            } else if (OpCode.MP_CREATE.getOpValue().equals(operation)) {
                String dst = workPath + "/TestThread-" + i + "/";
                task = new MultipartSubTask(src, dst, nameLists.get(i), fileSize, latch);
            } else {
                return;
            }
            threadPool.execute(task);
        }

        //wait result
        try {
            latch.await();
        } catch (InterruptedException e) {
            log.warn("CreateTask: execute task error,exception:", e);
        } finally {
            threadPool.shutdown();
            MetricsSystem.stopReport();
        }
    }

    public static Map<Integer, List<String>> prepareNamelists(int totalThreads, String nameType,
                                                         String filePrefix, int fileNumPerThread) {
        Map<Integer, List<String>> nameLists = new HashMap();
        if ("random".equals(nameType)) {
            for (int i = 0; i < totalThreads; i++) {
                List<String> names = new ArrayList<>();
                for (int j = 0; j < fileNumPerThread; j++) {
                    names.add(filePrefix + RandomStringUtils.randomAlphanumeric(10));
                }
                nameLists.put(i, names);
            }
        } else {
            for (int i = 0; i < totalThreads; i++) {
                int startNum = i * fileNumPerThread;
                int endNum = (i + 1) * fileNumPerThread;
                List<String> names = new ArrayList<>();
                for (int j = startNum; j < endNum; j++) {
                    names.add(filePrefix + "-" + j);
                }
                nameLists.put(i, names);
            }
        }
        return nameLists;
    }

    public static void prepareTestFile(long fileSize, String src) {
        try {
            File file = new File(src);
            FileOutputStream fileOut = new FileOutputStream(file, false);
            int bufferSize = 1024;
            while (fileSize > bufferSize) {
                String randomStr = RandomStringUtils.randomAlphanumeric(bufferSize);
                fileOut.write(randomStr.getBytes());
                fileSize -= bufferSize;
            }
            String randomStr = RandomStringUtils.randomAlphanumeric((int) fileSize);
            fileOut.write(randomStr.getBytes());
            fileOut.close();
        } catch (Exception e) {
            log.warn("when prepare test file meet exception:", e);
            throw new RuntimeException(e);
        }
    }

    public class CreateSubTask implements Runnable {
        private String src;
        private String dst;
        private String user;
        private List<String> nameList;
        private Configuration conf;
        private CountDownLatch Latch;
        private long fileSize;

        public CreateSubTask(String src, String dst, String user, List<String> nameList, long fileSize, Configuration conf,
                             CountDownLatch latch) {
            this.src = src;
            this.dst = dst;
            this.user = user;
            this.nameList = nameList;
            this.fileSize = fileSize;
            this.conf = conf;
            Latch = latch;
        }
        @Override
        public void run() {
            try {
                URI uri = new URI(dst);
                FileSystem fs = FileSystem.get(uri, conf, user);
                for (String name : nameList) {
                    String tmpdst = dst + name;
                    boolean ret;
                    try(Timer.Context context = timer.time()) {
                        ret = putToFS(src, tmpdst, fs);
                    }
                    if (!ret) {
                        log.warn("write : put file to hdfs failed, file:" + tmpdst);
                    } else {
                        iopsMeter.mark(fileSize);
                    }
                    qpsMeter.mark();
                }
            } catch (Exception e) {
                log.error("write task exception:", e);
            } finally {
                Latch.countDown();
            }
        }
    }

    public class RestCreateSubTask implements Runnable {
        private String src;
        private String bucket;
        private String dst;
        private String user;
        private List<String> nameList;
        private Configuration conf;
        private CountDownLatch Latch;
        private long fileSize;

        public RestCreateSubTask(String src, String bucket, String dst, String user, List<String> nameList, long fileSize, Configuration conf,
                             CountDownLatch latch) {
            this.src = src;
            this.bucket = bucket;
            this.dst = dst;
            this.user = user;
            this.nameList = nameList;
            this.fileSize = fileSize;
            this.conf = conf;
            Latch = latch;
        }
        @Override
        public void run() {
            try {
                boolean keepAlive = ToolConfig.getInstance().isTcpKeepAlive();
                String HostName = ToolConfig.getInstance().getRestHost();
                BasicAWSCredentials awsCreds = new BasicAWSCredentials(user, "secret_key_id");
                AmazonS3 s3 =
                    AmazonS3ClientBuilder.standard()
                        .withCredentials(new AWSStaticCredentialsProvider(awsCreds))
                        .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(
                            HostName, "xx"))
                        .withClientConfiguration(
                            new ClientConfiguration().withSocketTimeout(300 * 1000)
                                .withTcpKeepAlive(keepAlive))
                        .enablePathStyleAccess()
                        .build();
                for (String name : nameList) {
                    try(Timer.Context context = timer.time()) {
                        String objectKey = dst + name;
                        s3.putObject(bucket, objectKey, new File(src));
                        iopsMeter.mark(fileSize);
                        qpsMeter.mark();
                    } catch (Exception e) {
                        log.warn("write : put file to hdfs failed, file:" + name, e);
                    }
                }
            } catch (Exception e) {
                log.error("write task exception:", e);
            } finally {
                Latch.countDown();
            }
        }
    }

    public class MultipartSubTask implements Runnable {
        private String src;
        private String bucket;
        private String dst;
        private List<String> nameList;
        private CountDownLatch Latch;
        private long fileSize;
        private AmazonS3 s3Client;

        public MultipartSubTask(String src, String dst,
                                List<String> nameList, long fileSize, CountDownLatch latch) {
            this.src = src;
            this.dst = dst;
            this.bucket = ToolConfig.getInstance().getBucketName();
            this.nameList = nameList;
            this.fileSize = fileSize;
            Latch = latch;
            String HostName = ToolConfig.getInstance().getRestHost();
            String ak = ToolConfig.getInstance().getAccessKey();
            String sk = ToolConfig.getInstance().getSecretKey();
            // TODO: region could be configurable
            String region = "us-east-1";
            BasicAWSCredentials awsCreds = new BasicAWSCredentials(ak, sk);
            s3Client = AmazonS3ClientBuilder.standard()
                    .withCredentials(new AWSStaticCredentialsProvider(awsCreds))
                    .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(
                        HostName, region))
                    .enablePathStyleAccess()
                    .build();
        }
        @Override
        public void run() {
            try {
                runInternal();
                Snapshot snapshot = uploadPartTimer.getSnapshot();
                log.warn(String.format("upload part mean = %2.2f %s", convertDuration(snapshot.getMean()), getDurationUnit()));
                snapshot = completePartTimer.getSnapshot();
                log.warn(String.format("complete part mean = %2.2f %s", convertDuration(snapshot.getMean()), getDurationUnit()));
            } catch (Exception e) {
                log.error("write task exception:", e);
            } finally {
                Latch.countDown();
            }
        }

        private void runInternalWithCopyPart() {
            for (String name : nameList) {
                try(Timer.Context context = timer.time()) {
                    String objectKey = dst + name;
                    List<DeleteObjectsRequest.KeyVersion> keyVersions = new ArrayList<DeleteObjectsRequest.KeyVersion>(10);
                    for (int i=1; i<=10;i++) {
                        try (Timer.Context putPartCtx = uploadPartTimer.time()) {
                            String tempKey = objectKey + "-" + i;
                            s3Client.putObject(bucket, tempKey, new File(src));
                            iopsMeter.mark(fileSize);
                            keyVersions.add(new DeleteObjectsRequest.KeyVersion(tempKey));
                        }
                    }

                    try (Timer.Context cptPartCtx = completePartTimer.time()) {
                        InitiateMultipartUploadRequest initiateMPURequest =
                            new InitiateMultipartUploadRequest(bucket,
                                objectKey);
                        InitiateMultipartUploadResult initResponse =
                            s3Client.initiateMultipartUpload(initiateMPURequest);
                        String uploadId = initResponse.getUploadId();
                        List<PartETag> partETags = new ArrayList<>();
                        for (int i = 1; i <= 10; i++) {
                            CopyPartRequest copyPartRequest = new CopyPartRequest()
                                .withSourceBucketName(bucket)
                                .withSourceKey(objectKey + "-" + i)
                                .withDestinationBucketName(bucket)
                                .withDestinationKey(objectKey)
                                .withUploadId(uploadId)
                                .withPartNumber(i);
                            CopyPartResult copyResult = s3Client.copyPart(copyPartRequest);
                            partETags.add(copyResult.getPartETag());
                        }
                        CompleteMultipartUploadRequest compRequest =
                            new CompleteMultipartUploadRequest(bucket, objectKey, uploadId,
                                partETags);
                        CompleteMultipartUploadResult result =
                            s3Client.completeMultipartUpload(compRequest);
                        DeleteObjectsRequest request = new DeleteObjectsRequest(bucket)
                            .withKeys(keyVersions);
                        s3Client.deleteObjects(request);
                    }
                } catch (Exception e) {
                    log.warn("write : put file failed, file:" + name, e);
                }
            }
        }

        private void runInternal() {
            for (String name : nameList) {
                try(Timer.Context context = timer.time()) {
                    String objectKey = dst + name;
                    InitiateMultipartUploadRequest initiateMPURequest =
                        new InitiateMultipartUploadRequest(bucket,
                            objectKey);
                    InitiateMultipartUploadResult initResponse =
                        s3Client.initiateMultipartUpload(initiateMPURequest);
                    String uploadId = initResponse.getUploadId();
                    List<PartETag> partETags = new ArrayList<>();

                    for (int i = 1; i <= 5; i++) {
                        try(Timer.Context putPartCtx = uploadPartTimer.time()) {
                            UploadPartRequest uploadRequest = new UploadPartRequest()
                                .withBucketName(bucket)
                                .withKey(objectKey)
                                .withUploadId(uploadId)
                                .withPartNumber(i)
                                .withPartSize(fileSize)
                                .withFile(new File(src));
                            UploadPartResult uploadResult = s3Client.uploadPart(uploadRequest);
                            partETags.add(uploadResult.getPartETag());
                            iopsMeter.mark(fileSize);
                        }
                    }
                    try(Timer.Context cptPartCtx = completePartTimer.time()) {
                        CompleteMultipartUploadRequest compRequest =
                            new CompleteMultipartUploadRequest(bucket, objectKey, uploadId,
                                partETags);
                        CompleteMultipartUploadResult result =
                            s3Client.completeMultipartUpload(compRequest);
                    }
                    qpsMeter.mark();
                } catch (Exception e) {
                    log.warn("write : put file failed, file:" + name, e);
                }
            }
        }
    }
}
