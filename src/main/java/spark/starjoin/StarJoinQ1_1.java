package spark.starjoin;

import java.io.IOException;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import scala.Tuple2;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.JavaRDD;


public class StarJoinQ1_1
{
 
    public static void main(String[] args) throws IOException 
    {
        Parameters p = new Parameters();
       
        Configuration HDFSconf = new Configuration();    
        FileSystem fs = FileSystem.get(HDFSconf);
    
        long initTime = System.currentTimeMillis();
    
        SparkConf conf = new SparkConf().setAppName("StarJoin");
        if(p.useKryo)
        {    
            conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
            conf.set("spark.kryoserializer.buffer.mb","24");    
        }
        JavaSparkContext sc = new JavaSparkContext(conf);     
                 

        JavaPairRDD<String, String> dates = sc.textFile(p.datePath)
        .map( new Function<String, String[]>() {  public String[] call(String line) { return line.split("\\|"); } })
        .filter( new Function<String[], Boolean>() {  public Boolean call(String[] s) {  return s[4].equals("1993"); } })
        .mapToPair(new PairFunction<String[], String, String>() { public Tuple2<String, String> call(String[] s) { return new Tuple2<String, String>(s[0], null);}});
    
                
        JavaPairRDD<String, Long> lines = sc.textFile(p.linePath)
        .map( new Function<String, String[]>() {  public String[] call(String line) { return line.split("\\|"); } })
        .filter( new Function<String[], Boolean>() {  public Boolean call(String[] s) {  return Integer.parseInt(s[11]) <= 3 & Integer.parseInt(s[11]) >=1  & Integer.parseInt(s[8]) < 25; } })
        .mapToPair(new PairFunction<String[], String, Long>() { public Tuple2<String, Long> call(String[] s) 
        { return new Tuple2<String, Long>(s[5], Long.parseLong(s[9])*Long.parseLong(s[11]));}});  
        
        
         JavaPairRDD<String, Long> result = lines.join(dates)
        .mapToPair(new PairFunction<Tuple2 <String, Tuple2<Long, String>>, String, Long>() { public Tuple2<String, Long> call(Tuple2 <String,Tuple2<Long, String>> s)
        { return new Tuple2<String, Long>("1",s._2._1);}})
        .reduceByKey( new Function2<Long, Long, Long>() {  public Long call(Long i1, Long i2) {  return i1 + i2; }})
        .sortByKey();
        
        JavaRDD<Long> final_result = result.map(new Function<Tuple2<String, Long>, Long>() {  public Long call(Tuple2<String, Long> s) { return s._2; } } );
         
        fs.delete(new Path(p.home+"outputspark"), true);   
        final_result.saveAsTextFile(p.home+"outputspark");    
        long finalTime = System.currentTimeMillis(); 
        System.out.print("Tempo total(ms): ");
        System.out.println(finalTime - initTime);    
        sc.close();
    }
}
