//package sg.bigo.hdfs.task;
//
//import org.apache.hadoop.conf.Configuration;
//import org.apache.http.client.methods.CloseableHttpResponse;
//import org.apache.http.client.methods.HttpPost;
//import org.apache.http.entity.StringEntity;
//import org.apache.http.impl.client.CloseableHttpClient;
//import org.apache.http.impl.client.HttpClients;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//import sg.bigo.hdfs.common.HDFSConfig;
//import sg.bigo.hdfs.tool.BufInputStream;
//
//import java.io.*;
//import java.util.concurrent.CountDownLatch;
//import java.util.concurrent.ExecutorService;
//import java.util.concurrent.Executors;
//
//public class RestReadFileTask implements Runnable {
//
//    final static Logger log = LoggerFactory.getLogger(RestReadFileTask.class);
//
//    private String dst;
//    private int FileStartNum;
//    private int FileEndNum;
//    private Configuration conf;
//    private CountDownLatch latch;
//
//    public RestReadFileTask(String dst, int fileStartNum, int fileEndNum, Configuration conf, CountDownLatch latch) {
//        this.dst = dst;
//        FileStartNum = fileStartNum;
//        FileEndNum = fileEndNum;
//        this.conf = conf;
//        this.latch = latch;
//    }
//
//    public static void doTask(Configuration conf) {
//        int totalFiles = HDFSConfig.getInstance().getTotalFiles();
//        int totalThreads = HDFSConfig.getInstance().getTotalThreads();
//        int offset = HDFSConfig.getInstance().getFileOffset();
//        int SingleFileNum = totalFiles / totalThreads;
//        String HDFSDIR = HDFSConfig.getInstance().getWorkPath();
//        ExecutorService ThreadPool = Executors.newFixedThreadPool(totalThreads);
//        CountDownLatch Latch = new CountDownLatch(totalThreads);
//
//        try {
//            for (int i = 0; i < totalThreads; i++) {
//                int FileStartNum = i * SingleFileNum + offset;
//                int FileEndNum = (i + 1) * SingleFileNum;
//                //hadoop每个文件夹都有文件数量上限，所以此处为每个线程执行的上传新建一个目录
//                String dst = HDFSDIR + "/Thread-" + i + "/";
//                RestReadFileTask hlt = new RestReadFileTask(dst, FileStartNum, FileEndNum, conf, Latch);
//                ThreadPool.execute(hlt);
//            }
//            Latch.await();
//            ThreadPool.shutdown();
//        } catch (InterruptedException e) {
//            log.warn("CheckStatusTask: execute task error,exception:", e);
//        }
//    }
//
//    @Override
//    public void run() {
//        String putFilePrefix = HDFSConfig.getInstance().getWriteFilePrefix();
//        for (int n = FileStartNum; n < FileEndNum; n++) {
//            long start = System.currentTimeMillis();
//            long TTL = -1;
//            String tmpdst = dst + putFilePrefix + n;
//
////            String openUrl = "http://" + HDFSConfig.getInstance().getRestHost() + "/api/v1/paths/" + tmpdst + "/open-file";
////            String body = "{\"readType\":\"CACHE\"}";
////            Header header = new BasicHeader("Content-Type", "application/json");
////            HttpEntity open = ToolHttpClient.post(openUrl, body, header);
////            String streamid = "";
////            if (open != null) {
////                try {
////                    InputStream content = open.getContent();
////
////                    BufferedReader buffer = new BufferedReader(new InputStreamReader(content));
////                    String s = "";
////                    while ((s = buffer.readLine()) != null) {
////                        streamid += s;
////                    }
////                } catch (IOException e) {
////                    log.warn(tmpdst + " open req failed:", e);
////                }
////            } else {
////                continue;
////            }
////
////            if (StringUtils.isEmpty(streamid)) {
////                log.warn("path {} stream is empty.", tmpdst);
////                continue;
////            }
////            String readUrl = "http://" + HDFSConfig.getInstance().getRestHost() + "/api/v1/streams/" + streamid + "/read";
////            int total = 0;
////            HttpEntity read = ToolHttpClient.post(readUrl, null);
////            if (read != null) {
////                try {
////                    InputStream in = read.getContent();
////                    byte[] b = new byte[1048576];
////                    int length = 1048576;
////                    while (in.read(b, 0, length) > 0) {
////                        total += length;
////                    }
////                } catch (IOException e) {
////                    log.warn(tmpdst + " read stream failed:", e);
////                }
////            }
////
////            String closeUrl = "http://" + HDFSConfig.getInstance().getRestHost() + "/api/v1/streams/" + streamid + "/close";
////            ToolHttpClient.post(closeUrl, null);
////
////            TTL = System.currentTimeMillis() - start;
////            log.info("get file length[{}] ttl [{}]", total, TTL);
//
//
//
//            CloseableHttpClient httpClient = HttpClients.createDefault();
//            String openUrl = "http://" + HDFSConfig.getInstance().getRestHost() + "/api/v1/paths/" + tmpdst + "/open-file";
//            String body = "{\"readType\":\"CACHE\"}";
//            HttpPost openReq = new HttpPost(openUrl);
//            openReq.setHeader("Content-Type", "application/json");
//            openReq.addHeader("Connection", "Keep-Alive");
//            StringEntity se = null;
//            try {
//                se = new StringEntity(body);
//                se.setContentType("application/json");
//                openReq.setEntity(se);
//            } catch (UnsupportedEncodingException e) {
//                log.warn("content to entity error:", e);
//            }
//
//            CloseableHttpResponse openResp = null;
//            try {
//                openResp = httpClient.execute(openReq);
//            } catch (IOException ioe) {
//                log.warn(tmpdst + " open failed:", ioe);
//                continue;
//            }
//            String streamid = "";
//            try {
//                if (openResp != null) {
//                    InputStream content = openResp.getEntity().getContent();
//
//                    BufferedReader buffer = new BufferedReader(new InputStreamReader(content));
//                    String s = "";
//                    while ((s = buffer.readLine()) != null) {
//                        streamid += s;
//                    }
//                }
//            } catch (IOException e) {
//                log.warn(tmpdst + " read req failed:", e);
//            } finally {
//                try {
//                    openResp.close();
//                } catch (IOException ioe) {
//                    log.warn(tmpdst + " open req close failed:", ioe);
//                }
//            }
//
//            String readUrl = "http://" + HDFSConfig.getInstance().getRestHost() + "/api/v1/streams/" + streamid + "/read";
//            HttpPost readReq = new HttpPost(readUrl);
//            readReq.setHeader("Connection", "Keep-Alive");
//            CloseableHttpResponse readResp = null;
//            try {
//                readResp = httpClient.execute(readReq);
//            } catch (IOException e) {
//                log.warn(tmpdst + " read failed:", e);
//                continue;
//            }
//
//            int total = 0;
//            try {
//                if (readResp.getEntity() != null) {
//                    InputStream in = readResp.getEntity().getContent();
//                    InputStreamReader inputStreamReader = new InputStreamReader(in);
//                    BufferedReader bufferedReader = new BufferedReader(inputStreamReader);
//
//                    char[] b = new char[1024];
//                    int read;
//                    while ((read = bufferedReader.read(b,0, 1024)) > 0) {
//                        total += read;
//                    }
//                    in.close();
//                }
//            } catch (IOException e) {
//                log.warn(tmpdst + " read stream failed:", e);
//            } finally {
//                try {
//                    readResp.close();
//                } catch (IOException ioe) {
//                    log.warn(tmpdst + " read req close failed:", ioe);
//                }
//            }
//
//            String closeUrl = "http://" + HDFSConfig.getInstance().getRestHost() + "/api/v1/streams/" + streamid + "/close";
//            HttpPost closeReq = new HttpPost(closeUrl);
//            closeReq.setHeader("Connection", "Keep-Alive");
//
//            CloseableHttpResponse response = null;
//            try {
//                response = httpClient.execute(closeReq);
//            } catch (IOException e) {
//                log.warn(tmpdst + " close req failed:", e);
//            } finally {
//                if (response != null) {
//                    try {
//                        response.close();
//                    } catch (IOException ioe) {
//                        log.warn(tmpdst + " close req close failed:", ioe);
//                    }
//                }
//            }
//
//            TTL = System.currentTimeMillis() - start;
//            log.info("get file length[{}] ttl [{}]", total, TTL);
//            try {
//                httpClient.close();
//            } catch (IOException e) {
//                log.warn("httpClient close req close failed:", e);
//            }
//        }
//        latch.countDown();
//    }
//}
