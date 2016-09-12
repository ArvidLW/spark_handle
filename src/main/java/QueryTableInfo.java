package main.java;

import java.io.IOException;

import java.util.Arrays;
import java.util.List;       
        


import org.apache.hadoop.conf.Configuration;       
import org.apache.hadoop.hbase.HBaseConfiguration;       
import org.apache.hadoop.hbase.HTableDescriptor;

import org.apache.hadoop.hbase.client.HBaseAdmin;       

        
public class QueryTableInfo {         
           
    private static Configuration conf =null;    
     /**  
      * 初始化配置  
     */    
     static {
    	 /**
    	  * 首先在classpath下查找habse-site.xml文件，解析封装到Configuration对象，
    	  * 若不若在，则使用hbase-site.xml，
    	  * 还可以通过config.set(name,value)来手工构建Configuration对象。
    	  */
         conf = HBaseConfiguration.create();    
         System.out.println("执行完毕1");
     }
     public static void getDBDesc(){
    	 try {
    		 System.out.println("执行完毕2");
             HBaseAdmin admin =new HBaseAdmin(conf);
             String[] tableNames=admin.getTableNames();
             List<String> tbNameList=Arrays.asList(tableNames);
             HTableDescriptor[] hTableDescriptorArr = admin.getTableDescriptors(tbNameList);
             for (HTableDescriptor item:hTableDescriptorArr) {
                 System.out.println(item.getTableName());
                 System.out.println(item.getFamilies());
             }
    	 } catch (IOException e) {
			// TODO Auto-generated catch block
			System.out.println("执行完毕3");
			e.printStackTrace();
			
		}
     }
     public static void getTableDesc()
     {
         try {
             HBaseAdmin admin=new HBaseAdmin(conf);
         } catch (IOException e) {
             e.printStackTrace();
         }
     }
         
    /**
     * 创建一张表    
     */         
           
    public static void  main (String [] agrs) {
    	getDBDesc();
    	System.out.println("执行完毕4");
    }
}     
