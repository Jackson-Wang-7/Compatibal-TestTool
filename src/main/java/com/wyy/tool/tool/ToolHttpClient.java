package com.wyy.tool.tool;

import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;
import com.codahale.metrics.Meter;
import com.wyy.tool.common.ToolConfig;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.Header;
import org.apache.http.HeaderElement;
import org.apache.http.HeaderElementIterator;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.config.Registry;
import org.apache.http.config.RegistryBuilder;
import org.apache.http.conn.ConnectionKeepAliveStrategy;
import org.apache.http.conn.socket.ConnectionSocketFactory;
import org.apache.http.conn.socket.PlainConnectionSocketFactory;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.conn.ssl.TrustSelfSignedStrategy;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.DefaultHttpRequestRetryHandler;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.message.BasicHeaderElementIterator;
import org.apache.http.protocol.HTTP;
import org.apache.http.protocol.HttpContext;
import org.apache.http.ssl.SSLContextBuilder;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.security.KeyStore;

public class ToolHttpClient {
    private static Logger log = LoggerFactory.getLogger(ToolHttpClient.class);

    private static PoolingHttpClientConnectionManager poolConnManager = null;

    private final CloseableHttpClient httpClient;

    private final static ToolHttpClient INSTANCE = new ToolHttpClient();

    public ToolHttpClient() {
        try {
//            SSLContextBuilder builder = new SSLContextBuilder();
//            KeyStore truststore = KeyStore.getInstance("JKS");
//            try (FileInputStream fis = new FileInputStream(new File("/Users/wangyuyang/code/3-3/enterprise/keystore/truststore.jks"))) {
//                char[] truststorePassword = "trustpass".toCharArray();
//                truststore.load(fis, truststorePassword);
//            }
//            builder.loadTrustMaterial(truststore, null);

//            SSLConnectionSocketFactory sslsf = new SSLConnectionSocketFactory(builder.build());

            String trustStorePath = "/Users/wangyuyang/code/3-3/enterprise/keystore/truststore.jks";
            String trustStorePassword = "trustpass";
            SSLContext sslContext = createSslContext(trustStorePath, trustStorePassword);
            SSLConnectionSocketFactory sslSocketFactory = new SSLConnectionSocketFactory(sslContext);

            // 配置同时支持 HTTP 和 HTPPS
            Registry<ConnectionSocketFactory> socketFactoryRegistry =
                RegistryBuilder.<ConnectionSocketFactory>create()
                    .register("http", PlainConnectionSocketFactory.getSocketFactory())
                    .register("https", sslSocketFactory).build();
            // 初始化连接管理器
            poolConnManager = new PoolingHttpClientConnectionManager(socketFactoryRegistry);
            poolConnManager.setMaxTotal(1000);// 同时最多连接数
            // 设置最大路由
            poolConnManager.setDefaultMaxPerRoute(500);
        } catch (Exception e) {
            log.warn("http client init exception:", e);
        }
        httpClient = getConnection();
    }

    public static ToolHttpClient getInstance() {
        return INSTANCE;
    }

    private static final SSLContext createSslContext(String trustStorePath, String trustStorePassword) throws Exception {
        KeyStore trustStore = KeyStore.getInstance(KeyStore.getDefaultType());
        try (FileInputStream fis = new FileInputStream(trustStorePath)) {
            trustStore.load(fis, trustStorePassword.toCharArray());
        }

        TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        tmf.init(trustStore);

        SSLContext sslContext = SSLContext.getInstance("SSL");
        sslContext.init(null, tmf.getTrustManagers(), new java.security.SecureRandom());

        return sslContext;
    }

    private CloseableHttpClient getConnection() {
        ConnectionKeepAliveStrategy keepAliveStrategy = new ConnectionKeepAliveStrategy() {
            public long getKeepAliveDuration(HttpResponse response, HttpContext context) {
                // Honor 'keep-alive' header
                HeaderElementIterator it = new BasicHeaderElementIterator(
                    response.headerIterator(HTTP.CONN_KEEP_ALIVE));
                while (it.hasNext()) {
                    HeaderElement he = it.nextElement();
                    String param = he.getName();
                    String value = he.getValue();
                    if (value != null && param.equalsIgnoreCase("timeout")) {
                        try {
                            return Long.parseLong(value) * 1000;
                        } catch(NumberFormatException ignore) {
                        }
                    }
                }
                // otherwise keep alive for 30 seconds
                return 120 * 1000;
            }
        };
        RequestConfig config = RequestConfig.custom()
                .setConnectTimeout(60000)
                .setConnectionRequestTimeout(300000)
                .setSocketTimeout(300000)
                .build();
        CloseableHttpClient httpClient = HttpClients.custom()
                // 设置连接池管理
//                .setSSLSocketFactory()
                .setConnectionManager(poolConnManager)
                .setKeepAliveStrategy(keepAliveStrategy)
                .setDefaultRequestConfig(config)
                // 设置重试次数
                .setRetryHandler(new DefaultHttpRequestRetryHandler(3, false))
                .build();
        return httpClient;
    }

    public String httpGet(String url, Header... heads) {
        HttpGet httpGet = new HttpGet(url);
        CloseableHttpResponse response = null;

        try {
            if (heads != null) {
                httpGet.setHeaders(heads);
            }
            response = httpClient.execute(httpGet);
            String result = EntityUtils.toString(response.getEntity());
            int code = response.getStatusLine().getStatusCode();
            if (code == HttpStatus.SC_OK) {
                return result;
            } else {
                log.error("请求{}返回错误码：{},{}", url, code,result);
                return null;
            }
        } catch (IOException e) {
            log.error("http请求异常，{}",url,e);
        } finally {
            try {
                if (response != null)
                    response.close();
            } catch (IOException e) {
                log.warn("exception:", e);
            }
        }
        return null;
    }

    public boolean httpGetStream(String url, Meter ioMeter, Header... heads) {
        HttpGet httpGet = new HttpGet(url);
        CloseableHttpResponse response = null;

        try {
            if (heads != null) {
                httpGet.setHeaders(heads);
            }
            response = httpClient.execute(httpGet);
            int code = response.getStatusLine().getStatusCode();
            if (code == HttpStatus.SC_OK) {
                InputStream inputStream = response.getEntity().getContent();
                int bufferSize = ToolConfig.getInstance().getReadBufferSize();
                byte[] b = new byte[bufferSize];
                BufferedInputStream buffer = new BufferedInputStream(inputStream, bufferSize);
                int length = 0;
                while ((length = buffer.read(b)) > 0) {
                    ioMeter.mark(length);
                }
                return true;
            } else {
                String result = EntityUtils.toString(response.getEntity());
                log.error("request {} return error：{},{}", url, code,result);
                return false;
            }
        } catch (IOException e) {
            log.error("http request exception，{}",url,e);
        } finally {
            try {
                if (response != null)
                    response.close();
            } catch (IOException e) {
                log.warn("exception:", e);
            }
        }
        return false;
    }

    public String httpPost(String uri, String params, Header... heads) {
        HttpPost httpPost = new HttpPost(uri);
        CloseableHttpResponse response = null;
        try {
            if (StringUtils.isNotBlank(params)) {
                StringEntity paramEntity = new StringEntity(params);
                paramEntity.setContentEncoding("UTF-8");
                paramEntity.setContentType("application/json");
                httpPost.setEntity(paramEntity);
            }
            if (heads != null) {
                httpPost.setHeaders(heads);
            }
            response = httpClient.execute(httpPost);
            int code = response.getStatusLine().getStatusCode();
            String result = EntityUtils.toString(response.getEntity());
            if (code == HttpStatus.SC_OK) {
                return result;
            } else {
                log.error("请求{}返回错误码:{},请求参数:{}", uri, code, params);
                return null;
            }
        } catch (IOException e) {
            log.error("收集服务配置http请求异常", e);
        } finally {
            try {
                if(response != null) {
                    response.close();
                }
            } catch (IOException e) {
                log.warn("exception:", e);
            }
        }
        return null;
    }
}
