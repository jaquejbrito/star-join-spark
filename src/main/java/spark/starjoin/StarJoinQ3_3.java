package spark.starjoin;

import java.io.IOException;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import scala.Tuple2;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;

public class StarJoinQ3_3
{

    public static void main(String[] args) throws IOException 
    {
        Parameters param = new Parameters();
        long initTime = System.currentTimeMillis();
        
        SparkConf conf = new SparkConf().setAppName("StarJoin");
        JavaSparkContext sc = new JavaSparkContext(conf);
        
        if(param.useKryo)
        {    
            conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
            conf.set("spark.kryoserializer.buffer.mb","24");    
        }
       
        JavaPairRDD<String, String> supps = sc.textFile(param.suppPath)
        .map( new Function<String, String[]>() {  public String[] call(String line) { return line.split("\\|"); } })
        .filter( new Function<String[], Boolean>() {  public Boolean call(String[] s) {  return s[3].equals("UNITED KI1") | s[3].equals("UNITED KI5"); } })
        .mapToPair(new PairFunction<String[], String, String>() { public Tuple2<String, String> call(String[] s) {  return new Tuple2<String, String>(s[0], s[3]);}});
       
        JavaPairRDD<String, String> custs = sc.textFile(param.custPath)
        .map( new Function<String, String[]>() {  public String[] call(String line) { return line.split("\\|"); } })
        .filter( new Function<String[], Boolean>() {  public Boolean call(String[] s) {  return s[3].equals("UNITED KI1") | s[3].equals("UNITED KI5"); } })
        .mapToPair(new PairFunction<String[], String, String>() { public Tuple2<String, String> call(String[] s) {  return new Tuple2<String, String>(s[0], s[3]);}});
        
       
        JavaPairRDD<String, String> dates = sc.textFile(param.datePath)
        .map( new Function<String, String[]>() {  public String[] call(String line) { return line.split("\\|"); } })
        .filter( new Function<String[], Boolean>() {  public Boolean call(String[] s) {  return Integer.parseInt(s[4]) >= 1992  & Integer.parseInt(s[4]) <= 1997; } })        
        .mapToPair(new PairFunction<String[], String, String>() { public Tuple2<String, String> call(String[] s) { return new Tuple2<String, String>(s[0], s[4]);}});
    
      
        JavaPairRDD<String, String[]> lines = sc.textFile(param.linePath)
        .map( new Function<String, String[]>() {  public String[] call(String line) { return line.split("\\|"); } })
        .mapToPair(new PairFunction<String[], String, String[]>() { public Tuple2<String, String[]> call(String[] s){  String[] v = {s[2],s[5],s[12]}; return new Tuple2<String, String[]>(s[4], v);}});
    
      
        JavaPairRDD<String, String[]> result = lines.join(supps)
        .mapToPair(new PairFunction<Tuple2 <String, Tuple2<String[], String>>, String, String[]>() { public Tuple2<String, String[]> call(Tuple2 <String,Tuple2<String[], String>> s)
        { String[] v = {s._2._1[1],s._2._1[2],s._2._2}; return new Tuple2<String, String[]>(s._2._1[0], v);}});
        
        result = result.join(custs)
        .mapToPair(new PairFunction<Tuple2 <String, Tuple2<String[], String>>, String, String[]>() { public Tuple2<String, String[]> call(Tuple2 <String,Tuple2<String[], String>> s)
        { String[] v = {s._2._1[1],s._2._1[2],s._2._2}; return new Tuple2<String, String[]>(s._2._1[0], v);}});    
        
        JavaPairRDD<String,Long> final_result = result.join(dates)
        .mapToPair(new PairFunction<Tuple2 <String, Tuple2<String[], String>>, String, Long>() { public Tuple2<String, Long> call(Tuple2 <String,Tuple2<String[], String>> s)
        { return new Tuple2<String, Long>(s._2._1[2]+","+s._2._1[1]+","+s._2._2,Long.parseLong(s._2._1[0]));}})
        .reduceByKey( new Function2<Long, Long, Long>() {  public Long call(Long i1, Long i2) {  return i1 + i2; }});
        
        JavaPairRDD<String,String> sub_result = final_result
        .mapToPair( new PairFunction<Tuple2<String, Long>, String, String>() {  public Tuple2<String,String> call(Tuple2<String, Long> line) { 
        return new Tuple2(line._1+","+line._2.toString(),null); } });
        
        final_result = sub_result.sortByKey(new Q3Comparator())
        .mapToPair( new PairFunction<Tuple2<String,String>, String, Long>() {  public  Tuple2<String, Long> call(Tuple2<String,String> line) { 
        String[] s = line._1.split(",");   return new Tuple2<String, Long> (s[0]+","+s[1]+","+s[2], Long.parseLong(s[3])); } });
        
        Configuration HDFSconf = new Configuration();
        FileSystem fs = FileSystem.get(HDFSconf);
        fs.delete(new Path(param.home+"outputspark"), true);  
        
        final_result.saveAsTextFile(param.home+"outputspark");

        long finalTime = System.currentTimeMillis(); 
        System.out.print("Tempo total(ms): ");
        System.out.println(finalTime - initTime);

        sc.close();
    }
}