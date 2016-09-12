package main.java.myhadoop;

/**
 * Created by lw_co on 2016/8/18.
 */

import java.io.IOException;
import java.io.InputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

public class HdfsTest {

    private static Configuration conf=new Configuration();
    private static FileSystem fs;
    //private static Path path=new Path("hdfs://10.3.9.135:9000/");
    //static Configuration conf = new Configuration();
    //static FileSystem hdfs;
    static {
        try {
            Configuration conf = new Configuration();
            //conf.set("fs.defaultFS", "hdfs://10.3.9.135:9000/");
            //conf.set("hadoop.job.ugi", "devdpp01");
            //Path path = new Path("hdfs://10.3.9.135:9000/");

            fs = FileSystem.get(conf);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

//    static {
//
//        //conf.addResource("hdfs-site.xml");
//        //conf.addResource("core-site.xml");
//        conf.set("fs.defaultFS", "hdfs://10.3.9.135:9000/");
//        //conf.set("hadoop.job.ugi", "devdpp01");
//        Path path = new Path("hdfs://10.3.9.135:9000/");
//        //conf.addResource("mapred-site.xml");
//        //conf.set("mapred.jar", "./out/HdfsTest.jar"); //运行前程序要被打成JAR包
//        //conf.set("fs.default.name", "hdfs://10.3.9.135:9000");
//        //conf.set("mapred.job.tracker", "10.3.9.135:9001");
//
//        try {
//            fs=FileSystem.get(path.toUri(),conf);
//            System.out.println(fs.toString());
//            //fs=FileSystem.get()
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//    }

    //创建新文件
    public static void createFile(String dst , byte[] contents) throws IOException{
        //Configuration conf = new Configuration();
        System.out.println(conf.get("dfs.namenode.secondary.http-address"));
        //FileSystem fs = FileSystem.get(conf);
        Path dstPath = new Path(dst); //目标路径
        //打开一个输出流
        FSDataOutputStream outputStream = fs.create(dstPath);
        outputStream.write(contents);
        outputStream.close();
        fs.close();
        System.out.println("文件创建成功！");
    }

    //上传本地文件
    public static void uploadFile(String src,String dst) throws IOException{
        //Configuration conf = new Configuration();
        //FileSystem fs = FileSystem.get(conf);
        Path srcPath = new Path(src); //原路径
        Path dstPath = new Path(dst); //目标路径
        //调用文件系统的文件复制函数,前面参数是指是否删除原文件，true为删除，默认为false
        fs.copyFromLocalFile(false,srcPath, dstPath);

        //打印文件路径
        System.out.println("Upload to "+conf.get("fs.default.name"));
        System.out.println("------------list files------------"+"\n");
        FileStatus [] fileStatus = fs.listStatus(dstPath);
        for (FileStatus file : fileStatus)
        {
            System.out.println(file.getPath());
        }
        fs.close();
    }

    //文件重命名
    public static void rename(String oldName,String newName) throws IOException{
        //Configuration conf = new Configuration();
        //FileSystem fs = FileSystem.get(conf);
        Path oldPath = new Path(oldName);
        Path newPath = new Path(newName);
        boolean isok = fs.rename(oldPath, newPath);
        if(isok){
            System.out.println("rename ok!");
        }else{
            System.out.println("rename failure");
        }
        fs.close();
    }
    //删除文件
    public static void delete(String filePath) throws IOException{
       // Configuration conf = new Configuration();
        //FileSystem fs = FileSystem.get(conf);
        Path path = new Path(filePath);
        boolean isok = fs.deleteOnExit(path);
        if(isok){
            System.out.println("delete ok!");
        }else{
            System.out.println("delete failure");
        }
        fs.close();
    }

    //创建目录
    public static void mkdir(String path) throws IOException{
        //Configuration conf = new Configuration();
        //FileSystem fs = FileSystem.get(conf);
        Path srcPath = new Path(path);
        boolean isok = fs.mkdirs(srcPath);
        if(isok){
            System.out.println("create dir ok!");
        }else{
            System.out.println("create dir failure");
        }
        fs.close();
    }

    //读取文件的内容
    public static void readFile(String filePath) throws IOException{
       // Configuration conf = new Configuration();

        //conf.addResource("hdfs-site.xml");
        //conf.addResource("core-site.xml");
        System.out.println(conf.get("dfs.namenode.secondary.http-address"));
        //FileSystem fs = FileSystem.get(conf);
        Path srcPath = new Path(filePath);
        InputStream in = null;
        try {
            in = fs.open(srcPath);
            IOUtils.copyBytes(in, System.out, 4096, false); //复制到标准输出流
        } finally {
            IOUtils.closeStream(in);
        }
    }


    public static void main(String[] args) throws IOException {
        //测试上传文件
        //uploadFile("D:\\c.txt", "/user/hadoop/test/");
        //测试创建文件
        /*byte[] contents =  "hello world 世界你好\n".getBytes();
        //在本项目所在的根目录创建文件
        createFile("dd.txt",contents);*/
        //在本项目所在磁盘的根目录上创建文件了
        //createFile("/user111/hadoop11/test11/dd.txt",contents);
        //测试重命名
        //rename("/user/hadoop/test/d.txt", "/user/hadoop/test/dd.txt");
        //测试删除文件
        //delete("test/dd.txt"); //使用相对路径
        //delete("test1");    //删除目录
        //测试新建目录
        //mkdir("test1");
        //测试读取文件
        //readFile("D:/pythonspace/1.py");
        //readFile("lwtest/test.txt");//错误,读取本地文件系统/user/lw_co/lwtest/test.txt
        readFile("/lwtest/test.txt");//正确,读取hdfs上

    }

}