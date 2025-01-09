package com.wyy.tool.common;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

public class ToolConfig {
    private static ToolConfig config = new ToolConfig();

    private String host;
    private String restHost;
    private String opName;
    private String userName;
    private String workPath;
    private String bucketName;
    private String configPath;
    private int totalThreads;
    private int totalFiles;
    private int fileOffset;
    private String createFileNameType;
    private String createFilePath;
    private long createSizePerFile;
    private String createFilePrefix;
    private long readDurationTime;
    private int readBufferSize;
    private String deleteFilePrefix;
    private int mixReadPercentage;
    private int mixCreatePercentage;
    private String loopOps;
    private int loopCount;
    private String accessKey;
    private String secretKey;
    private long rangeSize;
    private boolean listThreadPrefix;
    private boolean tcpKeepAlive;

    public static ToolConfig getInstance() {
        return config;
    }

    public void loadProperties() throws IOException {
        Properties props = new Properties();
        props.load(new FileInputStream(Constants.properties));

        host = props.getProperty(Constants.KEY_HOST, Constants.VALUE_HOST_DEFAULT);
        restHost = props.getProperty(Constants.KEY_WEB_HOST, Constants.VALUE_WEB_HOST_DEFAULT);
        opName =
            props.getProperty(Constants.KEY_OPERATION_NAME, Constants.VALUE_OPERATION_NAME_DEFAULT);
        userName = props.getProperty(Constants.KEY_USER_NAME, Constants.VALUE_USER_NAME_DEFAULT);
        workPath = props.getProperty(Constants.KEY_WORK_PATH, Constants.VALUE_WORK_PATH_DEFAULT);
        bucketName =
            props.getProperty(Constants.KEY_BUCKET_NAME, Constants.VALUE_BUCKET_NAME_DEFAULT);
        configPath = props.getProperty(Constants.KEY_CONFIG_PATH, Constants.VALUE_CONFIG_DEFAULT);
        totalThreads = Integer.parseInt(
            props.getProperty(Constants.KEY_TOTAL_THREADS, Constants.VALUE_TOTAL_THREADS_DEFAULT));

        totalFiles = Integer.parseInt(
            props.getProperty(Constants.KEY_TOTAL_FILES, Constants.VALUE_TOTAL_FILES_DEFAULT));
        createFileNameType = props.getProperty(Constants.KEY_CREATE_FILE_NAME_TYPE,
            Constants.VALUE_CREATE_FILE_NAME_DEFAULT);
        createFilePath = props.getProperty(Constants.KEY_CREATE_FILE_PATH,
            Constants.VALUE_CREATE_FILE_PATH_DEFAULT);
        createSizePerFile = Long.parseLong(props.getProperty(Constants.KEY_CREATE_SIZE_PER_FILE,
            Constants.VALUE_CREATE_SIZE_PER_FILE_DEFAULT));
        createFilePrefix = props.getProperty(Constants.KEY_CREATE_FILE_PREFIX,
            Constants.VALUE_CREATE_FILE_PREFIX_DEFAULT);

        readDurationTime = Long.parseLong(props.getProperty(Constants.KEY_READ_DURATION_TIME,
            Constants.VALUE_READ_DURATION_TIME_DEFAULT));
        readBufferSize = Integer.parseInt(props.getProperty(Constants.KEY_READ_BUFFER_SIZE,
            Constants.VALUE_READ_BUFFER_SIZE_DEFAULT));
        deleteFilePrefix = props.getProperty(Constants.KEY_DELETE_FILE_PREFIX,
            Constants.VALUE_DELETE_FILE_PREFIX_DEFAULT);
        mixReadPercentage = Integer.parseInt(props.getProperty(Constants.KEY_MIX_READ_PERCENTAGE,
            Constants.VALUE_MIX_READ_PERCENTAGE));
        mixCreatePercentage = Integer.parseInt(
            props.getProperty(Constants.KEY_MIX_CREATE_PERCENTAGE,
                Constants.VALUE_MIX_CREATE_PERCENTAGE));
        loopOps = props.getProperty(Constants.KEY_OPS, Constants.VALUE_OPS_DEFAULT);
        loopCount = Integer.parseInt(
            props.getProperty(Constants.KEY_LOOP_COUNT, Constants.VALUE_LOOP_COUNT_DEFAULT));
        accessKey = props.getProperty(Constants.KEY_ACCESS_KEY, Constants.VALUE_ACCESS_KEY_DEFAULT);
        secretKey = props.getProperty(Constants.KEY_SECRET_KEY, Constants.VALUE_SECRET_KEY_DEFAULT);
        rangeSize = Integer.parseInt(
            props.getProperty(Constants.KEY_RANGE_SIZE, Constants.VALUE_RANGE_SIZE_DEFAULT));
        listThreadPrefix =
            Boolean.parseBoolean(props.getProperty(Constants.KEY_LIST_THREAD_PREFIX, "false"));
        tcpKeepAlive =
            Boolean.parseBoolean(props.getProperty(Constants.TCP_KEEP_ALIVE, "false"));
    }

    public String getHost() {
        return host;
    }

    public String getRestHost() {
        return restHost;
    }

    public String getOpName() {
        return opName;
    }

    public String getUserName() {
        return userName;
    }

    public String getWorkPath() {
        return workPath;
    }

    public String getConfigPath() {
        return configPath;
    }

    public int getTotalThreads() {
        return totalThreads;
    }

    public int getTotalFiles() {
        return totalFiles;
    }

    public String getCreateFileNameType() {
        return createFileNameType;
    }

    public long getCreateSizePerFile() {
        return createSizePerFile;
    }

    public int getFileOffset() {
        return fileOffset;
    }

    public String getCreateFilePath() {
        return createFilePath;
    }

    public String getCreateFilePrefix() {
        return createFilePrefix;
    }

    public long getReadDurationTime() {
        return readDurationTime;
    }

    public int getReadBufferSize() {
        return readBufferSize;
    }

    public String getDeleteFilePrefix() {
        return deleteFilePrefix;
    }

    public int getMixReadPercentage() {
        return mixReadPercentage;
    }

    public int getMixCreatePercentage() {
        return mixCreatePercentage;
    }

    public String getLoopOps() {
        return loopOps;
    }

    public int getLoopCount() {
        return loopCount;
    }

    public String getBucketName() {
        return bucketName;
    }

    public String getAccessKey() {
        return accessKey;
    }

    public String getSecretKey() {
        return secretKey;
    }

    public long getRangeSize() {
        return rangeSize;
    }

    public boolean isListThreadPrefix() {
        return listThreadPrefix;
    }

    public boolean isTcpKeepAlive() {
        return tcpKeepAlive;
    }
}
