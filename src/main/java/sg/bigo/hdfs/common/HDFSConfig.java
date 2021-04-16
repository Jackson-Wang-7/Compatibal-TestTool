package sg.bigo.hdfs.common;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

public class HDFSConfig {
    private static HDFSConfig config = new HDFSConfig();

    private String host;
    private String opName;
    private String workPath;
    private String configPath;
    private int totalThreads;
    private int totalFiles;
    private int fileOffset;
    private String putFilePath;
    private String writeFilePrefix;
    private String deleteFilePrefix;
    private String mixOps;
    private int loopCount;

    public static HDFSConfig getInstance() {
        return config;
    }

    public void loadProperties() throws IOException {
        Properties props = new Properties();
        props.load(new FileInputStream(Constants.properties));

        host = props.getProperty(Constants.KEY_HOST, Constants.VALUE_HOST_DEFAULT);
        opName = props.getProperty(Constants.KEY_OPERATION_NAME, Constants.VALUE_OPERATION_NAME_DEFAULT);
        workPath = props.getProperty(Constants.KEY_WORK_PATH, Constants.VALUE_WORK_PATH_DEFAULT);
        configPath = props.getProperty(Constants.KEY_CONFIG_PATH, Constants.VALUE_CONFIG_DEFAULT);
        totalFiles = Integer.parseInt(props.getProperty(Constants.KEY_TOTAL_FILES, Constants.VALUE_TOTAL_FILES_DEFAULT));
        totalThreads = Integer.parseInt(props.getProperty(Constants.KEY_TOTAL_THREADS, Constants.VALUE_TOTAL_THREADS_DEFAULT));
        fileOffset = Integer.parseInt(props.getProperty(Constants.KEY_FILE_OFFSET, Constants.VALUE_FILE_OFFSET_DEFAULT));
        putFilePath = props.getProperty(Constants.KEY_PUT_FILE_PATH, Constants.VALUE_PUT_FILE_PATH_DEFAULT);
        writeFilePrefix = props.getProperty(Constants.KEY_WRITE_FILE_PREFIX, Constants.VALUE_WRITE_FILE_PREFIX_DEFAULT);
        deleteFilePrefix = props.getProperty(Constants.KEY_DELETE_FILE_PREFIX, Constants.VALUE_DELETE_FILE_PREFIX_DEFAULT);
        mixOps = props.getProperty(Constants.KEY_OPS, Constants.VALUE_OPS_DEFAULT);
        loopCount = Integer.valueOf(props.getProperty(Constants.KEY_LOOP_COUNT, Constants.VALUE_LOOP_COUNT_DEFAULT));
    }

    public String getHost() {
        return host;
    }

    public String getOpName() {
        return opName;
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

    public int getFileOffset() {
        return fileOffset;
    }

    public String getPutFilePath() {
        return putFilePath;
    }

    public String getWriteFilePrefix() {
        return writeFilePrefix;
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
