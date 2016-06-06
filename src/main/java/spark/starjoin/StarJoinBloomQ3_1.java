package spark.starjoin;

import java.io.IOException;
import java.util.List;
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
import org.apache.spark.broadcast.Broadcast;

public class StarJoinBloomQ3_1
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
            conf.set("spark.kryo.registrator", MyBloomFilter.BloomFilterRegistrator.class.getName());
            conf.set("spark.kryoserializer.buffer.mb",param.buffer);    
        }
        
        MyBloomFilter.BloomFilter<String> BFS = new MyBloomFilter.BloomFilter(1.0, param.bitsS, param.hashes);
        MyBloomFilter.BloomFilter<String> BFD = new MyBloomFilter.BloomFilter(1.0, param.bitsD, param.hashes);
        MyBloomFilter.BloomFilter<String> BFC = new MyBloomFilter.BloomFilter(1.0, param.bitsC, param.hashes);

        JavaPairRDD<String, String> supps = sc.textFile(param.suppPath)
        .map( new Function<String, String[]>() {  public String[] call(String line) { return line.split("\\|"); } })
        .filter( new Function<String[], Boolean>() {  public Boolean call(String[] s) {  return s[5].equals("ASIA"); } })
        .mapToPair(new PairFunction<String[], String, String>() { public Tuple2<String, String> call(String[] s) {  return new Tuple2<String, String>(s[0], s[4]);}});
        
        
        List<Tuple2<String, String>> s = supps.collect();    
        for(int i=0; i< s.size(); i++)
        {      
           BFS.add(s.get(i)._1); 
        }
    
         final Broadcast<MyBloomFilter.BloomFilter<String>> varS = sc.broadcast(BFS);

        JavaPairRDD<String, String> custs = sc.textFile(param.custPath)
        .map( new Function<String, String[]>() {  public String[] call(String line) { return line.split("\\|"); } })
        .filter( new Function<String[], Boolean>() {  public Boolean call(String[] s) {  return s[5].equals("ASIA"); } })
        .mapToPair(new PairFunction<String[], String, String>() { public Tuple2<String, String> call(String[] s) {  return new Tuple2<String, String>(s[0], s[4]);}});
        
        List<Tuple2<String, String>> c = custs.collect();    
        for(int i=0; i< c.size(); i++)
        {      
           BFC.add(c.get(i)._1);
        }
    
         final Broadcast<MyBloomFilter.BloomFilter<String>> varC = sc.broadcast(BFC);
    
        JavaPairRDD<String, String> dates = sc.textFile(param.datePath)
        .map( new Function<String, String[]>() {  public String[] call(String line) { return line.split("\\|"); } })
        .filter( new Function<String[], Boolean>() {  public Boolean call(String[] s) {  return Integer.parseInt(s[4]) >= 1992  & Integer.parseInt(s[4]) <= 1997; } })        
        .mapToPair(new PairFunction<String[], String, String>() { public Tuple2<String, String> call(String[] s) { return new Tuple2<String, String>(s[0], s[4]);}});
    
        List<Tuple2<String, String>> d = dates.collect();    
        for(int i=0; i< d.size(); i++)
        {      
           BFD.add(d.get(i)._1);
        }
    
         final Broadcast<MyBloomFilter.BloomFilter<String>> varD = sc.broadcast(BFD);

        JavaPairRDD<String, String[]> lines = sc.textFile(param.linePath)
        .map( new Function<String, String[]>() {  public String[] call(String line) { return line.split("\\|"); } })
        .filter( new Function<String[], Boolean>() {  public Boolean call(String[] s) {  return varC.value().contains(s[2].getBytes()) & varS.value().contains(s[4].getBytes()) &  varD.value().contains(s[5].getBytes()); } })
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
        fs.delete(new Path(param.output), true);  
        
        final_result.saveAsTextFile(param.output);

        long finalTime = System.currentTimeMillis(); 
        System.out.print("Tempo total(ms): ");
        System.out.println(finalTime - initTime);

        sc.close();
    }
}