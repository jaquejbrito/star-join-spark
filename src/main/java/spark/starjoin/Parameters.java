package spark.starjoin;

public class Parameters {
    
final String suppPath = "hdfs:/user/hadoop/supplier/supplier.tbl";
final String datePath = "hdfs:/user/hadoop/date/date.tbl";    
final String partPath = "hdfs:/user/hadoop/part/part.tbl"; 
final String custPath = "hdfs:/user/hadoop/customer/customer.tbl"; 
final String linePath = "hdfs:/user/hadoop/lineorder/lineorder.tbl";
final String home = "/user/hadoop/";
final Integer bitsD=50000;
final Integer bitsC=75000000;
final Integer bitsP=30000000;
final Integer bitsS=10000000;
final Integer hashes =3;
final Boolean useKryo=true;
final String buffer = "64";
final String output = home+"output";

public Parameters ()
{
    
}        
    
}
