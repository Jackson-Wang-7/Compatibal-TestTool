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
    private String configPath;
    private int totalThreads;
    private int totalFiles;
    private int fileOffset;
    private String createFileNameType;
    private String createFilePath;
    private long createSizePerFile;
    private String createFilePrefix;
    private long readDurationTime;
    private String deleteFilePrefix;
    private String mixOps;
    private int loopCount;

    public static ToolConfig getInstance() {
        return config;
    }

    public void loadProperties() throws IOException {
        Properties props = new Properties();
        props.load(new FileInputStream(Constants.properties));

        host = props.getProperty(Constants.KEY_HOST, Constants.VALUE_HOST_DEFAULT);
        restHost = props.getProperty(Constants.KEY_WEB_HOST, Constants.VALUE_WEB_HOST_DEFAULT);
        opName = props.getProperty(Constants.KEY_OPERATION_NAME, Constants.VALUE_OPERATION_NAME_DEFAULT);
        userName = props.getProperty(Constants.KEY_USER_NAME, Constants.VALUE_USER_NAME_DEFAULT);
        workPath = props.getProperty(Constants.KEY_WORK_PATH, Constants.VALUE_WORK_PATH_DEFAULT);
        configPath = props.getProperty(Constants.KEY_CONFIG_PATH, Constants.VALUE_CONFIG_DEFAULT);
        totalThreads = Integer.parseInt(props.getProperty(Constants.KEY_TOTAL_THREADS, Constants.VALUE_TOTAL_THREADS_DEFAULT));

        totalFiles = Integer.parseInt(props.getProperty(Constants.KEY_TOTAL_FILES, Constants.VALUE_TOTAL_FILES_DEFAULT));
        createFileNameType = props.getProperty(Constants.KEY_CREATE_FILE_NAME_TYPE, Constants.VALUE_CREATE_FILE_NAME_DEFAULT);
        createFilePath = props.getProperty(Constants.KEY_CREATE_FILE_PATH, Constants.VALUE_CREATE_FILE_PATH_DEFAULT);
        createSizePerFile = Long.valueOf(props.getProperty(Constants.KEY_CREATE_SIZE_PER_FILE, Constants.VALUE_CREATE_SIZE_PER_FILE_DEFAULT));
        createFilePrefix = props.getProperty(Constants.KEY_CREATE_FILE_PREFIX, Constants.VALUE_CREATE_FILE_PREFIX_DEFAULT);

        readDurationTime = Long.valueOf(props.getProperty(Constants.KEY_READ_DURATION_TIME, Constants.VALUE_READ_DURATION_TIME_DEFAULT));
        deleteFilePrefix = props.getProperty(Constants.KEY_DELETE_FILE_PREFIX, Constants.VALUE_DELETE_FILE_PREFIX_DEFAULT);
        mixOps = props.getProperty(Constants.KEY_OPS, Constants.VALUE_OPS_DEFAULT);
        loopCount = Integer.valueOf(props.getProperty(Constants.KEY_LOOP_COUNT, Constants.VALUE_LOOP_COUNT_DEFAULT));
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

    public String getDeleteFilePrefix() {
        return deleteFilePrefix;
    }

    public String getMixOps() {
        return mixOps;
    }

    public int getLoopCount() {
        return loopCount;
    }
}
