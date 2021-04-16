package sg.bigo.hdfs.common;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class HdfsOperator {
    final static Log log = LogFactory.getLog(HdfsOperator.class);

    public static boolean putToHDFS(String src, String dst, FileSystem hdfs) {
        Path dstPath = new Path(dst);
        try {
            hdfs.copyFromLocalFile(false, new Path(src), dstPath);
        } catch (Exception ie) {
            log.error("Put file " + dst + " to HDFS failed! ", ie);
            return false;
        }
        return true;
    }

    public static FileStatus getFileInfo(String src, FileSystem hdfs) {
        try {
            FileStatus status = hdfs.getFileStatus(new Path(src));
            if (status == null) {
                log.warn("file" + src + " is not exist.");
                return null;
            }
            return status;
        } catch (Exception e) {
            log.error(" file " + src + " check failed! exception:", e);
            return null;
        }
    }

    public static List<String> listPrefix(String prefix, FileSystem hdfs) {
        try {
            FileStatus[] statusArray = hdfs.globStatus(new Path(prefix + "*"));
            List<String> paths = new ArrayList<>(statusArray.length);
            for(FileStatus s : statusArray) {
                String fileName = s.getPath().toUri().getPath();
                paths.add(fileName);
            }
            return paths;
        } catch (IOException e) {
            log.warn(prefix + " for Prefixlist error. exception:", e);
            return null;
        }
    }

    public static List<String> getListing(String src, FileSystem hdfs) {
        try {
            FileStatus[] status = hdfs.listStatus(new Path(src));
            List<String> paths = new ArrayList<>(status.length);
            for(FileStatus s : status) {
                String fileName = s.getPath().toUri().getPath();
                paths.add(fileName);
            }
            return paths;
        } catch (FileNotFoundException e) {
            log.warn(src + " file not found.");
            return null;
        } catch (IOException e) {
            log.warn(src + " list error. exception:", e);
            return null;
        }
    }

    public static boolean checkFile(String src, FileSystem hdfs) {
        try {
            FileStatus status = hdfs.getFileStatus(new Path(src));
            if (status == null) {
                log.warn("file" + src + " is not exist.");
                return false;
            }
        } catch (Exception e) {
            log.error(" file " + src + " check failed! exception:", e);
            return false;
        }
        return true;
    }

    public static boolean rename(String src, String dst, FileSystem hdfs) {
        try {
            Path srcPath = new Path(src);
            Path dstPath = new Path(dst);
            boolean ret = hdfs.rename(srcPath, dstPath);
            if (!ret) {
                String errorStr = "Error: rename file from " + src + " to " + dst + " failed.";
                log.error(errorStr);
                return ret;
            }
        } catch (Exception ie) {
            log.error("rename file from " + src + " to " + dst + " failed.e: ", ie);
            return false;
        }
        return true;
    }

    public static boolean delete(String src, FileSystem hdfs) {
        try {
            boolean ret = hdfs.delete(new Path(src), true);
            if (!ret) {
                log.error("delete file " + src + " failed");
                return ret;
            }
        } catch (Exception e) {
            log.error("delete file " + src + " failed.e: ", e);
            return false;
        }
        return true;
    }
}
