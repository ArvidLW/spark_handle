package main.java.myapp;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

import com.twitter.chill.Base64;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.mllib.clustering.KMeans;
import org.apache.spark.mllib.clustering.KMeansModel;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

/**
 * Example using MLlib KMeans from Java.
 */
public final class KMeansWeather {

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
        final Logger logger = LoggerFactory.getLogger(KMeansWeather.class);
        String f = "BaseInfo";
        String table = "NewCityWeather";
        Scan scan = new Scan();
        scan.addFamily(Bytes.toBytes(f));
        //scan.addColumn(Bytes.toBytes(f),Bytes.toBytes("temp"));

        Configuration conf = HBaseConfiguration.create();
        //conf.set("hbase.zookeeper.quorum","10.3.9.135,10.3.9.231,10.3.9.232");
        //conf.set("hbase.zookeeper.property.clientPort","2222");
        conf.set(TableInputFormat.INPUT_TABLE, table);
        conf.set(TableInputFormat.SCAN, Base64.encodeBytes(ProtobufUtil.toScan(scan).toByteArray()));
        //SparkConf confsp=new SparkConf().setAppName("SparkHBaseTest").setMaster("yarn-client");
        //SparkConf confsp=new SparkConf().setAppName("SparkHBaseTest").setMaster("spark://10.3.9.135:7077");
        //设置应用名称，就是在spark web端显示的应用名称，当然还可以设置其它的，在提交的时候可以指定，所以不用set上面两行吧
        SparkConf confsp = new SparkConf().setAppName("SparkHBaseTest");
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
        /**生成rdd*/
        JavaRDD<Vector> cityWeatherInfo = myRDD.map(new Function<Tuple2<ImmutableBytesWritable, Result>, Vector>() {
            @Override
            public Vector call(Tuple2<ImmutableBytesWritable, Result> immutableBytesWritableResultTuple2) throws Exception {
                double[] info = new double[5];
                byte[] v;
                v = immutableBytesWritableResultTuple2._2().getValue(
                        Bytes.toBytes("BaseInfo"), Bytes.toBytes("temp"));
                info[0] = Double.parseDouble(Bytes.toString(v));
                v = immutableBytesWritableResultTuple2._2().getValue(
                        Bytes.toBytes("BaseInfo"), Bytes.toBytes("wd"));
                info[1] = Double.parseDouble(Bytes.toString(v));
                v = immutableBytesWritableResultTuple2._2().getValue(
                        Bytes.toBytes("BaseInfo"), Bytes.toBytes("wse"));
                info[2] = Double.parseDouble(Bytes.toString(v));
                v = immutableBytesWritableResultTuple2._2().getValue(
                        Bytes.toBytes("BaseInfo"), Bytes.toBytes("pm"));
                info[3] = Double.parseDouble(Bytes.toString(v));
                v = immutableBytesWritableResultTuple2._2().getValue(
                        Bytes.toBytes("BaseInfo"), Bytes.toBytes("sd"));
                info[4] = Double.parseDouble(Bytes.toString(v));

                return Vectors.dense(info);


            }
        });
        List<Vector> output = cityWeatherInfo.collect();
        for (Vector ve : output) {
            //tuple._1();
            System.out.println(ve.toString());
        }

        //聚类算法kmeans
        KMeansModel model = KMeans.train(cityWeatherInfo.rdd(), 3, 10, 1, KMeans.K_MEANS_PARALLEL());

        System.out.println("Cluster centers:");
        for (Vector center : model.clusterCenters()) {
            System.out.println(" " + center);
        }
        //computeCost计算所有数据点到其最近的中心点的平方和来评估聚类，这个值越小聚类效果越好。
        // 当然还要考虑聚类结果的可解释性，例如k很大至于每个点都是中心点时，这是cost=0，则无意义。
        double cost = model.computeCost(cityWeatherInfo.rdd());
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
        /**可以用，mapToPair,生成新的pairrdd*/
        /*JavaPairRDD<String,String> myRDDVal=myRDD.mapToPair(new PairFunction<Tuple2<ImmutableBytesWritable, Result>, String, String>() {
            @Override
            public Tuple2<String, String> call(Tuple2<ImmutableBytesWritable, Result> immutableBytesWritableResultTuple2) throws Exception {
                byte[]  v= immutableBytesWritableResultTuple2._2().getValue(
                        Bytes.toBytes("BaseInfo"),Bytes.toBytes("temp") );
                if ( v!= null) {
                    return new Tuple2<String, String>("temp", Bytes.toString(v));
                }

                return null;
            }
        });*/
//        JavaPairRDD<String,Integer> myRDDVal=myRDD.map(new Function<Tuple2<ImmutableBytesWritable, Result>, R>() {
//            @Override
//            public R call(Tuple2<ImmutableBytesWritable, Result> immutableBytesWritableResultTuple2) throws Exception {
//                return null;
//            }
//        });
        //map改为mapToPair就好了
        /*JavaPairRDD<Integer, Integer> levels = myRDD.mapToPair(
                new PairFunction<Tuple2<ImmutableBytesWritable, Result>, Integer, Integer>() {
                    @Override
                    public Tuple2<Integer, Integer> call(
                            Tuple2<ImmutableBytesWritable, Result> immutableBytesWritableResultTuple2)
                            throws Exception {
                        byte[] o = immutableBytesWritableResultTuple2._2().getValue(
                                Bytes.toBytes("info"), Bytes.toBytes("levelCode"));
                        if (o != null) {
                            return new Tuple2<Integer, Integer>(Bytes.toInt(o), 1);
                        }
                        return null;
                    }
                });*/


/*        List<Tuple2<String, String>> output=myRDDVal.collect();
        for (Tuple2 tuple: output ) {
            //tuple._1();
            System.out.println(tuple._1+"："+tuple._2);
        }*/




//        JavaRDD rdd=JavaRDD.fromRDD(JavaPairRDD.toRDD(myRDD),myRDD.classTag());
//
//        rdd.map(new Function<String, Person>() {
//                    @Override
//                    public Person call(String line) throws Exception {
//                        String[] parts = line.split(",");
//                        Person person = new Person();
//                        person.setName(parts[0]);
//                        return person;
//                    }
//                });

//        System.out.println("lwwwww1:"+myRDD.map(new ParsePoint()));
//        logger.info("lwwwww1:"+myRDD.toString());
//        System.out.println("lwwwww2:"+rdd.toString());
//        logger.info("lwwwww2:"+rdd.toString());
//        JavaRDD<Vector> points = rdd.map(new ParsePoint());
////
//        KMeansModel model = KMeans.train(points.rdd(), 1, 100, 1, KMeans.K_MEANS_PARALLEL());
////
//        System.out.println("Cluster centers:");
//        for (Vector center : model.clusterCenters()) {
//            System.out.println(" " + center);
//        }
//        double cost = model.computeCost(points.rdd());
//        System.out.println("Cost: " + cost);
//
//        sc.stop();


    }

    public static void main(String[] args) throws IOException {
        getValueFromHB();
//        if (args.length < 3) {
//            System.err.println(
//                    "Usage: JavaKMeans <input_file> <k> <max_iterations> [<runs>]");
//            System.exit(1);
//        }
//        String inputFile = args[0];
//        int k = Integer.parseInt(args[1]);
//        int iterations = Integer.parseInt(args[2]);
//        int runs = 1;
//
//        if (args.length >= 4) {
//            runs = Integer.parseInt(args[3]);
//        }
//        SparkConf sparkConf = new SparkConf().setAppName("JavaKMeans");
//        JavaSparkContext sc = new JavaSparkContext(sparkConf);
//        JavaRDD<String> lines = sc.textFile(inputFile);
//
//        JavaRDD<Vector> points = lines.map(new ParsePoint());
//
//        KMeansModel model = KMeans.train(points.rdd(), k, iterations, runs, KMeans.K_MEANS_PARALLEL());
//
//        System.out.println("Cluster centers:");
//        for (Vector center : model.clusterCenters()) {
//            System.out.println(" " + center);
//        }
//        double cost = model.computeCost(points.rdd());
//        System.out.println("Cost: " + cost);
//
//        sc.stop();
    }
}