package com.wyy.tool.tool;

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
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

public class ToolHttpClient {
    private static Logger log = LoggerFactory.getLogger(ToolHttpClient.class);

    private static PoolingHttpClientConnectionManager poolConnManager = null;

    private static CloseableHttpClient httpClient;
    static {
        try {
            SSLContextBuilder builder = new SSLContextBuilder();
            builder.loadTrustMaterial(null, new TrustSelfSignedStrategy());
            SSLConnectionSocketFactory sslsf = new SSLConnectionSocketFactory(builder.build());
            // ?????????????????? HTTP ??? HTPPS
            Registry<ConnectionSocketFactory> socketFactoryRegistry = RegistryBuilder.<ConnectionSocketFactory>create().register("http", PlainConnectionSocketFactory.getSocketFactory()).register("https", sslsf).build();
            // ????????????????????????
            poolConnManager = new PoolingHttpClientConnectionManager(socketFactoryRegistry);
            poolConnManager.setMaxTotal(640);// ?????????????????????
            // ??????????????????
            poolConnManager.setDefaultMaxPerRoute(320);
            // ???????????????MaxtTotal???DefaultMaxPerRoute????????????
            // 1???MaxtTotal???????????????????????????
            // 2???DefaultMaxPerRoute??????????????????????????????MaxTotal???????????????????????????
            // MaxtTotal=400 DefaultMaxPerRoute=200
            // ??????????????????http://www.abc.com??????????????????????????????????????????200????????????400???
            // ???????????????http://www.bac.com ???
            // http://www.ccd.com??????????????????????????????????????????200??????????????????400??????????????????400?????????????????????????????????DefaultMaxPerRoute
            // ?????????httpClient
            httpClient = getConnection();
        } catch (Exception e) {
            log.warn("http client init exception:", e);
        }
    }

    public static CloseableHttpClient getConnection() {
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
                .setConnectTimeout(5000)
                .setConnectionRequestTimeout(5000)
                .setSocketTimeout(300000)
                .build();
        CloseableHttpClient httpClient = HttpClients.custom()
                // ?????????????????????
                .setConnectionManager(poolConnManager)
                .setKeepAliveStrategy(keepAliveStrategy)
                .setDefaultRequestConfig(config)
                // ??????????????????
                .setRetryHandler(new DefaultHttpRequestRetryHandler(3, false))
                .build();
        return httpClient;
    }

    public static String httpGet(String url, Header... heads) {
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
                log.error("??????{}??????????????????{},{}", url, code,result);
                return null;
            }
        } catch (IOException e) {
            log.error("http???????????????{}",url,e);
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

    public static boolean httpGetStream(String url, Meter ioMeter, Header... heads) {
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
                log.error("request {} return error???{},{}", url, code,result);
                return false;
            }
        } catch (IOException e) {
            log.error("http request exception???{}",url,e);
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

    public static String httpPost(String uri, String params, Header... heads) {
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
                log.error("??????{}???????????????:{},????????????:{}", uri, code, params);
                return null;
            }
        } catch (IOException e) {
            log.error("??????????????????http????????????", e);
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
