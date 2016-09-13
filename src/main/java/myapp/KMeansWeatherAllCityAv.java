package main.java.myapp;

import com.twitter.chill.Base64;
import main.java.myTools.CleanData;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.mllib.clustering.KMeans;
import org.apache.spark.mllib.clustering.KMeansModel;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Byte;
import scala.Tuple2;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

/**
 * Example using MLlib KMeans from Java.
 */
public final class KMeansWeatherAllCityAv {

    private static class ParsePoint implements Function<String, Vector> {
        private static final Pattern SPACE = Pattern.compile(" ");

        @Override
        public Vector call(String line) {
            String[] tok = SPACE.split(line);
            double[] point = new double[tok.length];
            for (int i = 0; i < tok.length; ++i) {
                point[i] = Double.parseDouble(tok[i]);
            }
            return Vectors.dense(point);
        }
    }
    public static void getValueFromHB() throws IOException {
        final Logger logger = LoggerFactory.getLogger(KMeansWeatherAllCityAv.class);
        String f = "cf";
        String table = "CityWeather";
        Scan scan = new Scan();
        scan.addFamily(Bytes.toBytes(f));
        scan.setMaxVersions(24);//怎么感觉设没没都是scan那么多呢43238。
        //scan.addColumn(Bytes.toBytes(f),Bytes.toBytes("temp"));
        //Filter filter=new SingleColumnValueFilter(Bytes.toBytes(f),Bytes.toBytes("Info"), CompareFilter.CompareOp.NOT_EQUAL,Bytes.toBytes("null"));
        //scan.setFilter();

        Configuration conf = HBaseConfiguration.create();
        //conf.set("hbase.zookeeper.quorum","10.3.9.135,10.3.9.231,10.3.9.232");
        //conf.set("hbase.zookeeper.property.clientPort","2222");
        conf.set(TableInputFormat.INPUT_TABLE, table);
        conf.set(TableInputFormat.SCAN, Base64.encodeBytes(ProtobufUtil.toScan(scan).toByteArray()));
        //SparkConf confsp=new SparkConf().setAppName("SparkHBaseTest").setMaster("yarn-client");
        //SparkConf confsp=new SparkConf().setAppName("SparkHBaseTest").setMaster("spark://10.3.9.135:7077");
        //设置应用名称，就是在spark web端显示的应用名称，当然还可以设置其它的，在提交的时候可以指定，所以不用set上面两行吧
        SparkConf confsp = new SparkConf().setAppName("SparkCityWeatherKMeans");
        //.setMaster("local")//以本地的形式运行
        //.setJars(new String[]{"D:\\jiuzhouwork\\workspace\\hbase_handles\\out\\artifacts\\hbase_handles_jar\\hbase_handles.jar"});
        //创建spark操作环境对象
        JavaSparkContext sc = new JavaSparkContext(confsp);
//        JavaSparkContext sc = new JavaSparkContext("yarn-client", "hbaseTest",
//                System.getenv("SPARK_HOME"), System.getenv("JARS"));
        //sc.addJar("D:\\jiuzhouwork\\other\\sparklibex\\spark-examples-1.6.1-hadoop2.7.1.jar");
        //从数据库中获取查询内容生成RDD
        JavaPairRDD<ImmutableBytesWritable, Result> myRDD = sc.newAPIHadoopRDD(conf, TableInputFormat.class, ImmutableBytesWritable.class, Result.class);

//        JavaPairRDD<String,Integer> myRDDVal=myRDD.flatMapToPair(new PairFlatMapFunction<Tuple2<ImmutableBytesWritable, Result>, String, Integer>() {
//            @Override
//            public Iterable<Tuple2<String, Integer>> call(Tuple2<ImmutableBytesWritable, Result> immutableBytesWritableResultTuple2) throws Exception {
//                return null;
//            }
//        });
         /*JavaRDD<String> myInfo=myRDD.map(new Function<Tuple2<ImmutableBytesWritable, Result>, String>() {
            @Override
            public String call(Tuple2<ImmutableBytesWritable, Result> immutableBytesWritableResultTuple2) throws Exception {
                byte[]  v= immutableBytesWritableResultTuple2._2().getValue(
                        Bytes.toBytes("BaseInfo"),Bytes.toBytes("temp") );
                if ( v!= null) {
                    return Bytes.toString(v);
                }

                return null;
            }
        });
        List<String> output=myInfo.collect();
        for (String s: output ) {
            //tuple._1();
            System.out.println(s);
        }*/
        /**字节转double要通过String才行，不知道为什么，不能直接用toDouble转，好奇怪！*/
        /*JavaRDD<Double> myInfo=myRDD.map(new Function<Tuple2<ImmutableBytesWritable, Result>, Double>() {
            @Override
            public Double call(Tuple2<ImmutableBytesWritable, Result> immutableBytesWritableResultTuple2) throws Exception {
                byte[]  v= immutableBytesWritableResultTuple2._2().getValue(
                        Bytes.toBytes("BaseInfo"),Bytes.toBytes("temp") );
                if ( v!= null) {
                    return Double.parseDouble(Bytes.toString(v));
                }

                return null;
            }
        });
        List<Double> output=myInfo.collect();
        for (Double s: output ) {
            //tuple._1();
            System.out.println(s);
        }*/
        /*JavaRDD<Vector> myInfo=myRDD.map(new Function<Tuple2<ImmutableBytesWritable, Result>, Vector>() {
            @Override
            public Vector call(Tuple2<ImmutableBytesWritable, Result> immutableBytesWritableResultTuple2) throws Exception {
                byte[]  v= immutableBytesWritableResultTuple2._2().getValue(
                        Bytes.toBytes("BaseInfo"),Bytes.toBytes("temp") );


                if ( v!= null) {
                    double[] ss=new double[1];
                    System.out.println("wwwww2222");
                    ss[0]=Bytes.toDouble(v);
                    System.out.println("wwwww3333");
                    return Vectors.dense(ss);
                }

                return null;
            }
        });
        System.out.println("wwwww111");
        List<Vector> output=myInfo.collect();
        for (Vector ve: output ) {
            //tuple._1();
            System.out.println(ve.toArray().toString());
        }*/
        System.out.println("wwwwwwww:"+myRDD.count());
        JavaPairRDD<String,Vector> cityWeatherInfo2=myRDD.mapToPair(new PairFunction<Tuple2<ImmutableBytesWritable, Result>, String, Vector>() {
            @Override
            public Tuple2<String, Vector> call(Tuple2<ImmutableBytesWritable, Result> immutableBytesWritableResultTuple2) throws Exception {
                String row_id=Bytes.toString(immutableBytesWritableResultTuple2._2().getRow());
                double[] dInfo=new double[5];
                byte[] v;
                v = immutableBytesWritableResultTuple2._2().getValue(
                        Bytes.toBytes(f), Bytes.toBytes("Info"));
                //JSONObject dataJson=new JSONObject(Bytes.toString(v));
                JSONObject weatherJson=new JSONObject((new JSONObject(Bytes.toString(v)) ).get("weatherinfo").toString());

                String[] sval={weatherJson.get("temp").toString(),weatherJson.get("wd").toString(),weatherJson.get("wse").toString(),weatherJson.get("pm").toString(),weatherJson.get("sd").toString()};


                for(int i=0;i<dInfo.length;++i)
                {
                    if(sval[i].matches("^[0-9].*"))
                    {

                        if(i==4){
                            //由于第4个是形如：40%
                            dInfo[i]=Double.parseDouble(CleanData.sdToNumStr(sval[i]));
                        }
                        else {
                            dInfo[i]=Double.parseDouble(sval[i]);
                        }

                    }
                    else if(sval[i]!=null && !sval[i].isEmpty() && !"null".equals(sval[i]) && !"?".equals(sval[i])){
                        dInfo[i]=(double) CleanData.directionToNum(sval[i]);
                    }
                    else {
                        return new Tuple2<>(row_id,null);
                    }
                }


                return new Tuple2<>(row_id,Vectors.dense(dInfo));
            }
        });
        System.out.println("cityWeatherInfo2:"+cityWeatherInfo2.count());
        JavaPairRDD<String,Vector> cityWeatherInfo3=cityWeatherInfo2.filter(new Function<Tuple2<String, Vector>, Boolean>() {
            @Override
            public Boolean call(Tuple2<String, Vector> stringVectorTuple2) throws Exception {
                return stringVectorTuple2._2!=null;
            }
        }).reduceByKey(new Function2<Vector, Vector, Vector>() {
            @Override
            public Vector call(Vector vector, Vector vector2) throws Exception {
                double[] v1=vector.toArray();
                double[] v2=vector2.toArray();
                for(int i=0;i<v1.length;++i)
                {
                    v1[i]=(v1[i]+v2[i])/2;
                }
                return Vectors.dense(v1);
            }
        });
        System.out.println("cityWeatherInfo3:"+cityWeatherInfo3.count());
        /*JavaPairRDD<String,Vector> cityWeatherInfo2=myRDD.map(new Function<Tuple2<ImmutableBytesWritable, Result>, R>() {
            @Override
            public R call(Tuple2<ImmutableBytesWritable, Result> immutableBytesWritableResultTuple2) throws Exception {
                return null;
            }
        });*/
        /**生成新的rdd*/
//        JavaRDD<Vector> cityWeatherInfo1 = myRDD.map(new Function<Tuple2<ImmutableBytesWritable, Result>, Vector>() {
//            @Override
//            public Vector call(Tuple2<ImmutableBytesWritable, Result> immutableBytesWritableResultTuple2) throws Exception {
//                double[] info = new double[5];
//                byte[] v;
//                String s;
//                v = immutableBytesWritableResultTuple2._2().getValue(
//                        Bytes.toBytes(f), Bytes.toBytes("Info"));
//                //JSONObject dataJson=new JSONObject(Bytes.toString(v));
//                JSONObject weatherJson=new JSONObject((new JSONObject(Bytes.toString(v)) ).get("weatherinfo").toString());
//
//                s=weatherJson.get("temp").toString();
//                if(s!=null && !s.isEmpty() && s.matches("^[0-9].*"))
//                {info[0] = Double.parseDouble(s);}else{return null;}
//
//                s=weatherJson.get("wd").toString();
//                if(s!=null && !s.isEmpty() && !"null".equals(s) && !"?".equals(s))
//                {info[1] = (double) CleanData.directionToNum(s);}else{return null;}
//
//                s=weatherJson.get("wse").toString();
//                if(s!=null && !s.isEmpty() && s.matches("^[0-9].*"))
//                {info[2] = Double.parseDouble(s);}else{return null;}
//
//                s=weatherJson.get("pm").toString();
//                //logger.info("lllll:"+s);
//                //System.out.println("lllll:"+s);
//                if(s!=null && !s.isEmpty() && s.matches("^[0-9].*"))
//                {info[3] = Double.parseDouble(s);}else{return null;}
//
//                s=weatherJson.get("sd").toString();
//                if(s!=null && !s.isEmpty() && s.matches("^[0-9].*"))
//                {info[4] = Double.parseDouble(CleanData.sdToNumStr(s));}else{return null;}
//
//                return Vectors.dense(info);
//
//
//            }
//        });
//        JavaRDD<Vector> cityWeatherInfo=cityWeatherInfo1.filter(new Function<Vector, Boolean>() {
//            @Override
//            public Boolean call(Vector vector) throws Exception {
//                return vector!=null;
//            }
//        });
//
//
//        List<Vector> output = cityWeatherInfo.collect();
//        for (Vector ve : output) {
//            //tuple._1();
//            if(ve !=null)
//            {System.out.println(ve.toString());}
//            else{
//                System.out.println("the null");
//            }
//        }
//
//
        //聚类算法kmeans
        KMeansModel model = KMeans.train(cityWeatherInfo3.values().rdd(), 3, 10, 1, KMeans.K_MEANS_PARALLEL());

        System.out.println("Cluster centers:");
        for (Vector center : model.clusterCenters()) {
            System.out.println(" " + center);
        }
        //computeCost计算所有数据点到其最近的中心点的平方和来评估聚类，这个值越小聚类效果越好。
        // 当然还要考虑聚类结果的可解释性，例如k很大至于每个点都是中心点时，这是cost=0，则无意义。
        double cost = model.computeCost(cityWeatherInfo3.values().rdd());
        System.out.println("Cost: " + cost);

        //预测值，这里用的原来的值
        double[][] dds = new double[][]{

                {12.0, 8.0, 1.0, 105.0, 87.0},
                {14.0, 3.0, 1.0, 27.0, 63.0},
                {17.0, 1.0, 0.0, 40.0, 38.0},
                {17.0, 2.0, 1.0, 20.0, 56.0},
                {21.0, 3.0, 1.0, 98.0, 49.0},
                {19.0, 3.0, 1.0, 60.0, 42.0},
                {20.0, 2.0, 1.0, 57.0, 52.0},
                {19.0, 2.0, 1.0, 110.0, 56.0},
                {20.0, 3.0, 2.0, 84.0, 61.0},
                {18.0, 2.0, 1.0, 72.0, 65.0},
                {18.0, 2.0, 1.0, 79.0, 77.0},
                {18.0, 5.0, 2.0, 70.0, 74.0},
                {22.0, 6.0, 2.0, 27.0, 50.0},
                {18.0, 3.0, 2.0, 68.0, 78.0},
                {20.0, 2.0, 1.0, 26.0, 64.0},
                {21.0, 1.0, 1.0, 24.0, 50.0},
                {24.0, 1.0, 1.0, 31.0, 56.0},
                {26.0, 2.0, 1.0, 54.0, 58.0}
        };
        for (double[] d: dds) {
            System.out.println("vector "+ Arrays.toString(d)+" belongs to clustering "+model.predict(Vectors.dense(d)));
        }


        sc.stop();

    }

    public static void main(String[] args) throws IOException {
        getValueFromHB();
    }
}