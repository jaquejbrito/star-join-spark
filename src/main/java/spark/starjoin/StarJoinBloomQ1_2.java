package spark.starjoin;

import java.io.IOException;
import java.util.List;
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
import org.apache.spark.api.java.JavaRDD;



public class StarJoinBloomQ1_2
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
            conf.set("spark.kryo.registrator", MyBloomFilter.BloomFilterRegistrator.class.getName());
            conf.set("spark.kryoserializer.buffer.mb",p.buffer);    
        }
        JavaSparkContext sc = new JavaSparkContext(conf);     
                 
        MyBloomFilter.BloomFilter<String> BFD = new MyBloomFilter.BloomFilter(1.0, p.bitsD, p.hashes);
        
        JavaPairRDD<String, String> dates = sc.textFile(p.datePath)
        .map( new Function<String, String[]>() {  public String[] call(String line) { return line.split("\\|"); } })
        .filter( new Function<String[], Boolean>() {  public Boolean call(String[] s) {  return  s[5].equals("199401");  } })
        .mapToPair(new PairFunction<String[], String, String>() { public Tuple2<String, String> call(String[] s) { return new Tuple2<String, String>(s[0], null);}});
    
        List<Tuple2<String, String>> a = dates.collect();    
        Integer i=0, imax=a.size();    
        while(i < imax)        
        {      
           BFD.add(a.get(i)._1); 
           i++;
        }
    
        final Broadcast<MyBloomFilter.BloomFilter<String>> varD = sc.broadcast(BFD);     
        
        JavaPairRDD<String, Long> lines = sc.textFile(p.linePath)
        .map( new Function<String, String[]>() {  public String[] call(String line) { return line.split("\\|"); } })
        .filter( new Function<String[], Boolean>() {  public Boolean call(String[] s) {  return Integer.parseInt(s[11]) <= 6 & Integer.parseInt(s[11]) >= 4  & Integer.parseInt(s[8]) <= 35 & Integer.parseInt(s[8]) >= 26 & varD.value().contains(s[5].getBytes()); } })
        .mapToPair(new PairFunction<String[], String, Long>() { public Tuple2<String, Long> call(String[] s) 
        { return new Tuple2<String, Long>(s[5], Long.parseLong(s[9])*Long.parseLong(s[11]));}});  
        
        
         JavaPairRDD<String, Long> result = lines.join(dates)
        .mapToPair(new PairFunction<Tuple2 <String, Tuple2<Long, String>>, String, Long>() { public Tuple2<String, Long> call(Tuple2 <String,Tuple2<Long, String>> s)
        { return new Tuple2<String, Long>("1",s._2._1);}})
        .reduceByKey( new Function2<Long, Long, Long>() {  public Long call(Long i1, Long i2) {  return i1 + i2; }})
        .sortByKey();
        
        JavaRDD<Long> final_result = result.map(new Function<Tuple2<String, Long>, Long>() {  public Long call(Tuple2<String, Long> s) { return s._2; } } );
         
        fs.delete(new Path(p.output), true);   
        final_result.saveAsTextFile(p.output);    
        long finalTime = System.currentTimeMillis(); 
        System.out.print("Tempo total(ms): ");
        System.out.println(finalTime - initTime);    
        sc.close();
    }
}
