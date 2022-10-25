package com.wyy.tool.common;

import org.apache.commons.codec.binary.Hex;
import org.apache.commons.codec.binary.StringUtils;
import org.apache.hadoop.fs.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;

public class ToolOperator {
    final static Logger log = LoggerFactory.getLogger(ToolOperator.class);

    public static boolean putToFS(String src, String dst, FileSystem fs) {
        Path dstPath = new Path(dst);
        try {
            fs.copyFromLocalFile(false, new Path(src), dstPath);
        } catch (Exception ie) {
            log.error("Put file " + dst + " to HDFS failed! ", ie);
            return false;
        }
        return true;
    }

    public static FileStatus getFileInfo(String src, FileSystem fs) {
        try {
            FileStatus status = fs.getFileStatus(new Path(src));
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

    public static List<String> listPrefix(String prefix, FileSystem fs) {
        try {
            FileStatus[] statusArray = fs.globStatus(new Path(prefix + "*"));
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

    public static List<String> getListing(String src, FileSystem fs) {
        try {
            FileStatus[] status = fs.listStatus(new Path(src));
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

    public static boolean readFile(String src, FileSystem fs) {
        long TTL;
        long start = System.currentTimeMillis();
        Path srcPath = new Path(src);
        byte[] b = new byte[1048576];
        int total = 0;
        int length;
        FSDataInputStream in = null;
        try {
            in = fs.open(srcPath);
            while ((length = in.read(b)) > 0) {
                total = total + length;
            }
            TTL = System.currentTimeMillis() - start;
            log.info("get file length[{}] ttl [{}]", total, TTL);
        } catch (IOException e) {
            log.error("read file " + src + " failed! ", e);
            return false;
        } finally {
            if (in != null) {
                try {
                    in.close();
                } catch (IOException ioException) {
                    log.warn("read file " + src + " close stream failed. ");
                }
            }
        }
        return true;
    }

    public static boolean readFileToLocal(String src, String dst, FileSystem fs) {
        try {
            Path srcPath = new Path(src);
            byte[] b = new byte[1048576];
            int total = 0;
            int length;

            FSDataInputStream in = fs.open(srcPath);
            File file = new File(dst);
            FileOutputStream fileOut = new FileOutputStream(file, true);
            while ((length = in.read(b)) > 0) {
                fileOut.write(b, 0, length);
                total = total + length;
            }
            log.warn("path [{}] read size: {}.", src, total);
        } catch (IOException e) {
            log.error("read file " + src + " failed! ", e);
            return false;
        }
        return true;
    }
    /**
     * 用于diff操作
     * @param hdfsPath HDFS中的文件路径
     * @param localFilePath 本地文件路径
     * @param fs HDFS文件系统
     * @return
     */
    public static boolean readFileCheck(String hdfsPath, String localFilePath, FileSystem fs) {
        Path srcPath = new Path(hdfsPath);
        int bufferSize = 1 * 1024 * 1024;
        byte[] bufferFromHdfs = new byte[bufferSize];
        byte[] localBuffer = new byte[bufferSize];
        int total = 0;
        int len = 0;
        FSDataInputStream in = null;
        FileInputStream fileIn = null;
        try {
            MessageDigest md5forhdfs = MessageDigest.getInstance("MD5");
            MessageDigest md5forlocal = MessageDigest.getInstance("MD5");

            in = fs.open(srcPath);
            // 本地上传的文件
            File file = new File(localFilePath);
            fileIn = new FileInputStream(file);

            while (-1 != (len = in.read(bufferFromHdfs, 0, bufferSize))) {
                md5forhdfs.update(bufferFromHdfs, 0, len);
                total += len;
            }

            while (-1 != (len = fileIn.read(localBuffer, 0, bufferSize))) {
                md5forlocal.update(localBuffer, 0, len);
            }
            String hdfsMd5 = new String(Hex.encodeHex(md5forhdfs.digest()));
            String localMd5 = new String(Hex.encodeHex(md5forlocal.digest()));
            if (!StringUtils.equals(hdfsMd5, localMd5)) {
                log.error("file {} is in inconsistent state ", hdfsPath);
            } else {
                log.info("read file {} length {} success.", hdfsPath, total);
            }
        } catch (IOException | NoSuchAlgorithmException e) {
            log.error("read file " + hdfsPath + " failed! ", e);
            return false;
        } finally {
            if (in != null) {
                try {
                    in.close();
                } catch (IOException ioException) {
                    log.warn("read file " + hdfsPath + " close stream failed. ");
                }
            }

            if (fileIn != null) {
                try {
                    fileIn.close();
                } catch (IOException ioException) {
                    //do nothing
                }
            }
        }
        return true;
    }

    public static boolean diffMd5(String hdfsPath,String md5,FileSystem fs) {

        Path srcPath = new Path(hdfsPath);
        int bufferSize = 1 * 1024 * 1024;
        byte[] bufferFromHdfs = new byte[bufferSize];
        int total = 0;
        int len = 0;
        FSDataInputStream in = null;
        try {
            in = fs.open(srcPath);
            MessageDigest md5forhdfs = MessageDigest.getInstance("MD5");
            while (-1 != (len = in.read(bufferFromHdfs, 0, bufferSize))) {
                md5forhdfs.update(bufferFromHdfs, 0, len);
                total += len;
            }
            String hdfsMd5 = new String(Hex.encodeHex(md5forhdfs.digest()));
            if (!StringUtils.equals(hdfsMd5, md5)) {
                log.error("file {} is in inconsistent state ", hdfsPath);
            } else {
                log.debug("read file {} length {} success.", hdfsPath, total);
            }
        } catch (IOException | NoSuchAlgorithmException e) {
            log.error("read file " + hdfsPath + " failed! ", e);
            return false;
        } finally {
            if (in != null) {
                try {
                    in.close();
                } catch (IOException ioException) {
                    log.warn("read file " + hdfsPath + " close stream failed. ");
                }
            }
        }
        return true;
    }

    public static String getLocalFileMd5(String filePath) throws IOException {
        int bufferSize = 1 * 1024 * 1024;
        byte[] localBuffer = new byte[1024 * 1024];
        // 本地上传的文件
        File file = new File(filePath);
        FileInputStream fileIn = null;
        try {
            fileIn = new FileInputStream(file);
            int len=0;

            MessageDigest md5forlocal = MessageDigest.getInstance("MD5");
            while (-1 != (len = fileIn.read(localBuffer, 0, bufferSize))) {
                md5forlocal.update(localBuffer, 0, len);
            }
            return new String(Hex.encodeHex(md5forlocal.digest()));
        } catch (IOException | NoSuchAlgorithmException e) {
            log.error("read local file " + filePath + " failed! ", e);
            throw new IOException(e);
        } finally {
            if (fileIn != null) {
                fileIn.close();
            }
        }
    }

    public static long getLocalFileSize(String filePath) {
        File file = new File(filePath);
        if (!file.exists() || !file.isFile()) {
            log.warn("file {} is not exist.", filePath);
            return -1;
        }
        return file.length();
    }

    public static boolean preadFileToLocal(String src, String dst, FileSystem fs) {
        try {
            Path srcPath = new Path(src);
            byte[] b = new byte[1048576];
            int position = 0;
            int length;

            FSDataInputStream in = fs.open(srcPath);
            File file = new File(dst);
            FileOutputStream fileOut = new FileOutputStream(file, true);
            while ((length = in.read(position, b, 0 ,1048576)) > 0) {
                fileOut.write(b, 0, length);
                position = position + length;
            }
            log.warn("path [{}] read size: {}.", src, position);
        } catch (IOException e) {
            log.error("read file " + src + " failed! ", e);
            return false;
        }
        return true;
    }

    public static boolean preadFileCheck(String src, String dst, FileSystem fs) {
        try {
            Path srcPath = new Path(src);
            byte[] bufferFromHdfs = new byte[1048576];
            byte[] localBuffer = new byte[1048576];
            int position = 0;
            int length;

            FSDataInputStream in = fs.open(srcPath);
            File file = new File(dst);
//            FileOutputStream fileOut = new FileOutputStream(file, true);
            FileInputStream fileIn = new FileInputStream(file);
            while ((length = in.read(position, bufferFromHdfs, 0 ,1048576)) > 0) {
                fileIn.read(localBuffer, 0, length);
                // todo  比较 bufferFromHdfs的内容和localBuffer的内容

                position = position + length;
            }
            log.warn("path [{}] read size: {}.", src, position);
        } catch (IOException e) {
            log.error("read file " + src + " failed! ", e);
            return false;
        }
        return true;
    }

    public static boolean checkFile(String src, FileSystem fs) {
        try {
            FileStatus status = fs.getFileStatus(new Path(src));
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

    public static boolean rename(String src, String dst, FileSystem fs) {
        try {
            Path srcPath = new Path(src);
            Path dstPath = new Path(dst);
            boolean ret = fs.rename(srcPath, dstPath);
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

    public static boolean delete(String src, FileSystem fs) {
        try {
            boolean ret = fs.delete(new Path(src), true);
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
