package main.java.myhadoop;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

public class HdfsTest1 {
    // initialization
    static Configuration conf = new Configuration();
    static FileSystem hdfs;
    static {
        try {
            Configuration conf = new Configuration();
            conf.set("fs.defaultFS", "hdfs://10.3.9.135:9000/");
            //conf.set("hadoop.job.ugi", "devdpp01");
            Path path = new Path("hdfs://10.3.9.135:9000/");

            hdfs = FileSystem.get(path.toUri(), conf);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    // create a direction
    public void createDir(String dir) throws IOException {
        Path path = new Path(dir);
        if (hdfs.exists(path)) {
            System.out.println("dir \t" + conf.get("fs.default.name") + dir
                    + "\t already exists");
            return;
        }
        hdfs.mkdirs(path);
        System.out.println("new dir \t" + conf.get("fs.default.name") + dir);
    }
    // copy from local file to HDFS file
    public void copyFile(String localSrc, String hdfsDst) throws IOException {
        Path src = new Path(localSrc);
        Path dst = new Path(hdfsDst);
        if (!(new File(localSrc)).exists()) {
            System.out.println("Error: local dir \t" + localSrc
                    + "\t not exists.");
            return;
        }
        if (!hdfs.exists(dst)) {
            System.out.println("Error: dest dir \t" + dst.toUri()
                    + "\t not exists.");
            return;
        }
        String dstPath = dst.toUri() + "/" + src.getName();
        if (hdfs.exists(new Path(dstPath))) {
            System.out.println("Warn: dest file \t" + dstPath
                    + "\t already exists.");
        }
        hdfs.copyFromLocalFile(src, dst);
        // list all the files in the current direction
        FileStatus files
                [] = hdfs.listStatus(dst);
        System.out.println("Upload to \t" + conf.get("fs.default.name")
                + hdfsDst);
        for (FileStatus file : files) {
            System.out.println(file.getPath());
        }
    }
    // create a new file
    public void createFile(String fileName, String fileContent)
            throws IOException {
        Path dst = new Path(fileName);
        byte[] bytes = fileContent.getBytes();
        FSDataOutputStream output = hdfs.create(dst);
        output.write(bytes);
        System.out.println("new file \t" + conf.get("fs.default.name")
                + fileName);
    }
    // create a new file
    public void appendFile(String fileName, String fileContent)
            throws IOException {
        Path dst = new Path(fileName);
        byte[] bytes = fileContent.getBytes();
        if (!hdfs.exists(dst)) {
            createFile(fileName, fileContent);
            return;
        }
        FSDataOutputStream output = hdfs.append(dst);
        output.write(bytes);
        System.out.println("append to file \t" + conf.get("fs.default.name")
                + fileName);
    }
    // list all files
    public void listFiles(String dirName) throws IOException {
        Path f = new Path(dirName);
        FileStatus[] status = hdfs.listStatus(f);
        System.out.println(dirName + " has all files:");
        for (int i = 0; i < status.length; i++) {
            System.out.println(status[i].getPath().toString());
        }
    }
    // judge a file existed? and delete it!
    public void deleteFile(String fileName) throws IOException {
        Path f = new Path(fileName);
        boolean isExists = hdfs.exists(f);
        if (isExists) { // if exists, delete
            boolean isDel = hdfs.delete(f, true);
            System.out.println(fileName + "  delete? \t" + isDel);
        } else {
            System.out.println(fileName + "  exist? \t" + isExists);
        }
    }
    public void readFile(String filePath) throws IOException{
        //FileSystem fs = FileSystem.get(conf);
        Path srcPath = new Path(filePath);
        InputStream in = null;
        in=hdfs.open(srcPath);
        IOUtils.copyBytes(in, System.out, 4096, false);
        IOUtils.closeStream(in);
//        try {
//            in = hdfs.open(srcPath);
//            IOUtils.copyBytes(in, System.out, 4096, false); //复制到标准输出流
//        } finally {
//            IOUtils.closeStream(in);
//        }
    }

    public static void main(String[] args) throws IOException {
        HdfsTest1 ofs = new HdfsTest1();
        //System.out.println("\n=======create dir=======");
//        String dir = "/lwtest";
//        // ofs.createDir(dir);
//        // System.out.println("\n=======copy file=======");
//        String src = "D:/jiuzhouwork/other/test22.txt";
//        ofs.copyFile(src, dir);
        ofs.readFile("/lwtest/test.txt");
        //ofs.listFiles("/lwtest");
        // System.out.println("\n=======create a file=======");
        // String fileContent = "Hello, world! Just a test.";
        // ofs.appendFile(dir+"/word.txt", fileContent);
        //String contents =  "hello world 世界你好\n";
        //在本项目所在的根目录创建文件
        //ofs.createFile("dd.txt",contents);
    }
}
