package main.java.myapp;


import main.java.myTools.CleanData;
import main.java.myhbase.HBaseClientImpl;

import org.apache.hadoop.hbase.util.Bytes;

import org.codehaus.jackson.map.JsonSerializer;
import org.codehaus.jackson.map.MappingJsonFactory;
import org.codehaus.jackson.map.util.JSONPObject;
import org.codehaus.jettison.json.JSONObject;


import java.util.*;

/**
 * Created by lw_co on 2016/8/22.
 */
public class ScanHandleWeatherData {
    private static List<List<byte[]>> valList;
    public static void main(String[] args) throws Exception {
//        Configuration conf= HBaseConfiguration.create();
//        HTable hTable=new HTable(conf,"CityWeather");
//        Get g=new Get(Bytes.toBytes("cf"));//hbase以字节码存储所以这要转化为字节码
//        g.setMaxVersions(1);//得到最大一个版本，不调用时默认也这么多,如果不带参数表示取所有
        String tablename="LWCityWeather";
        String columnFamily="BaseInfo";//这里只有一列
        HBaseClientImpl hclient=new HBaseClientImpl("CityWeather");
        valList=hclient.scanValues("cf","Info");
        if(! hclient.createTable(tablename,new String[]{columnFamily},1))
        {
            return;
        }
        //System.out.println(Bytes.toString(valList.get(10)));
        for(List<byte[]> l:valList)
        {
            byte[] rowid=l.get(0);
            byte[] b=l.get(1);
            //System.out.println(Bytes.toString(b));
            String data=Bytes.toString(b);
            JSONObject dataJson=new JSONObject(data);
            String weatherinfo= dataJson.get("weatherinfo").toString();
            //System.out.println(weatherinfo);

            JSONObject mydata=new JSONObject(weatherinfo);

            Map map=new HashMap();
            map.put("city",mydata.get("city").toString());
            map.put("date_y",mydata.get("date_y").toString());
            map.put("temp",mydata.get("temp").toString());
            map.put("wd",Integer.toString(CleanData.directionToNum(mydata.get("wd").toString())));
            map.put("wse",mydata.get("wse").toString());
            map.put("pm",mydata.get("pm").toString());
            map.put("sd",CleanData.sdToNumStr(mydata.get("sd").toString()));
            Map mymap=new HashMap();

            map.put("base",map.toString());

            hclient.mapAddRecords(tablename,Bytes.toString(rowid),columnFamily,map);

            //byte[]

//            String[] myweather=new String[7];
//            myweather[0]=mydata.get("city").toString();
//            myweather[1]=mydata.get("date_y").toString();
//            myweather[2]=mydata.get("temp").toString();
//            myweather[3]=Integer.toString(CleanData.directionToNum(mydata.get("wd").toString()));
//            myweather[4]=mydata.get("wse").toString();
//            myweather[5]=mydata.get("pm").toString();
//            myweather[6]=CleanData.sdToNumStr(mydata.get("sd").toString());
//            System.out.println(Bytes.toString(rowid)+Arrays.deepToString(myweather));
//            hclient.addRecords(tablename,rowid,columnFamily,);

//          针对多重值的
//            for(byte[] b:l)
//            {
//
//                //System.out.println(Bytes.toString(b));
//                String data=Bytes.toString(b);
//                JSONObject dataJson=new JSONObject(data);
//                String weatherinfo= dataJson.get("weatherinfo").toString();
//                //System.out.println(weatherinfo);
//
//                JSONObject mydata=new JSONObject(weatherinfo);
//                String[] myweather=new String[7];
//                myweather[0]=mydata.get("city").toString();
//                myweather[1]=mydata.get("date_y").toString();
//                myweather[2]=mydata.get("temp").toString();
//                myweather[3]=Integer.toString(CleanData.directionToNum(mydata.get("wd").toString()));
//                myweather[4]=mydata.get("wse").toString();
//                myweather[5]=mydata.get("pm").toString();
//                myweather[6]=CleanData.sdToNumStr(mydata.get("sd").toString());
//                System.out.println(Arrays.deepToString(myweather));
//
//                //地区+时间存储
//            }
        }
    }
}
