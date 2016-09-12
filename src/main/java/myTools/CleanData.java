package main.java.myTools;

/**
 * Created by lw_co on 2016/8/22.
 */
public class CleanData {
    public static int directionToNum(String d)
    {
        if (d.equals("北风")) {
            return 1;
        }else if (d.equals("东北风")){
            return 2;
        }else if (d.equals("东风")){
            return 3;
        }else if (d.equals("东南风")){
            return 4;
        }else if (d.equals("南风")){
            return 5;
        }else if (d.equals("西南风")){
            return 6;
        }else if (d.equals("西风")){
            return 7;
        }else if (d.equals("西北风")){
            return 8;
        }else{
            return 0;
        }
    }
    public static String sdToNumStr(String s){
        return s.substring(0, s.length()-1);
    }
}
