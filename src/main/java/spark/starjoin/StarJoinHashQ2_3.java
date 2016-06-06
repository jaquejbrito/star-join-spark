package spark.starjoin;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import scala.Tuple2;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.broadcast.Broadcast;
import org.apache.hadoop.fs.Path;


public class StarJoinHashQ2_3
{
 
    public static void main(String[] args) throws IOException 
    {
        
        Map<Integer, String> hashMapS = new HashMap();
        Map<Integer, String> hashMapP = new HashMap();
        Map<Integer, String> hashMapD = new HashMap();
  
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
                 
        JavaPairRDD<String, String> supps = sc.textFile(p.suppPath).map( new Function<String, String[]>() {  public String[] call(String s) { return s.split("\\|"); } })
        .filter( new Function<String[], Boolean>() { public Boolean call(String[] s){ return s[5].equals("EUROPE"); } })
        .mapToPair(new PairFunction<String[], String, String>() { public Tuple2<String, String> call(String[] s) {  return new Tuple2<String, String>(s[0], null);}});
    
        List<Tuple2<String, String>> a = supps.collect();    
        Integer i=0, imax=a.size();    
        while(i < imax)        
        {      
            hashMapS.put(Integer.parseInt(a.get(i)._1),null);        
            i++;
        }
         
        final Broadcast<Map<Integer, String>> varS = sc.broadcast(hashMapS); 
    
       
        
        JavaPairRDD<String, String> dates = sc.textFile(p.datePath)
        .map( new Function<String, String[]>() {  public String[] call(String line) { return line.split("\\|"); } })
        .mapToPair(new PairFunction<String[], String, String>() { public Tuple2<String, String> call(String[] s) { return new Tuple2<String, String>(s[0], s[4]);}});
    
        List<Tuple2<String, String>> d = dates.collect();    
        Integer m=0, mmax=d.size();    
        while(m < mmax)        
        {      
            hashMapD.put(Integer.parseInt(d.get(m)._1),d.get(m)._2);           
            m++;
        }
    
        final Broadcast<Map<Integer, String>> varD = sc.broadcast(hashMapD);     

        JavaPairRDD<String, String> parts = sc.textFile(p.partPath)
        .map( new Function<String, String[]>() {  public String[] call(String line) { return line.split("\\|"); } })
        .filter( new Function<String[], Boolean>() {  public Boolean call(String[] s) {  return s[4].equals("MFGR#2221");  } })
        .mapToPair(new PairFunction<String[], String, String>() { public Tuple2<String, String> call(String[] s) { return new Tuple2<String, String>(s[0], s[4]);}});
    
        List<Tuple2<String, String>> c = parts.collect();    
        Integer k=0, kmax=c.size();    
        while(k < kmax)        
        {      
            hashMapP.put(Integer.parseInt(c.get(k)._1),c.get(k)._2);       

            k++;
        }
    
        final Broadcast<Map<Integer, String>> varP = sc.broadcast(hashMapP);     
    
        JavaPairRDD<String, Long> lines = sc.textFile(p.linePath)
        .map( new Function<String, String[]>() {  public String[] call(String line) { return line.split("\\|"); } })
        .filter( new Function<String[], Boolean>() {  public Boolean call(String[] s) {  return varS.value().containsKey(Integer.parseInt(s[4])) && varP.value().containsKey(Integer.parseInt(s[3])) && varD.value().containsKey(Integer.parseInt(s[5])); } })
        .mapToPair(new PairFunction<String[], String, Long>() { public Tuple2<String, Long> call(String[] s) 
        { return new Tuple2<String, Long>(varD.value().get(Integer.parseInt(s[5]))+","+varP.value().get(Integer.parseInt(s[3])), Long.parseLong(s[12]));}});
  
        JavaPairRDD<String, Long> final_result =lines.reduceByKey( new Function2<Long, Long, Long>() {  public Long call(Long i1, Long i2) {  return i1 + i2; }});
        final_result = final_result.sortByKey();
        
        fs.delete(new Path(p.home+"outputspark"), true);   
        final_result.saveAsTextFile(p.home+"outputspark");    
        long finalTime = System.currentTimeMillis(); 
        System.out.print("Tempo total(ms): ");
        System.out.println(finalTime - initTime);    
        sc.close();
    }
}
