package spark.starjoin;

import java.io.Serializable;
import java.util.Comparator;

public class Q3Comparator implements Comparator<String>, Serializable {
        @Override
        public int compare(String s1, String s2) {
            String [] st1 = s1.split(",");
            String [] st2 = s2.split(",");
           
            Long x1, x2;
            x1 = Long.parseLong(st1[3]);
            x2 = Long.parseLong(st2[3]);
            int compare = st1[2].compareTo(st2[2]);

            if (compare == 0)
            {            
                return  x2.compareTo(x1);
            }
            return compare;
        }
    }