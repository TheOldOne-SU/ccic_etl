package net.ccic.sparkprocess.hive.mis;

import org.apache.hadoop.hive.ql.exec.UDF;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;

/**
 * Created by FrankSen on 2018/11/20.
 */
public class RecentTimeMetrics extends UDF {


    SimpleDateFormat sdf = new SimpleDateFormat ("yyyy-MM-dd HH:mm:ss");

    Calendar c = Calendar.getInstance();



    public String evaluate() throws ParseException {
        return nowDateMetric();
    }

    /**
     *
     * @param nowDate
     * @return
     */
    public String nowDateMetric() throws ParseException {

        c.setTime(c.getTime());

        int minutes = c.get(Calendar.MINUTE);

        if(minutes >= 0 && minutes < 15){
            c.set(Calendar.MINUTE, 0);
        }else if(minutes >= 15 && minutes < 30){
            c.set(Calendar.MINUTE, 15);
        }else if(minutes >= 30 && minutes < 45){
            c.set(Calendar.MINUTE, 30);
        }else if(minutes >= 45 && minutes < 60){
            c.set(Calendar.MINUTE, 45);
        }
        c.set(Calendar.SECOND,0);

        return sdf.format(c.getTime());
    }



    //public static void main(String[] args) throws ParseException {
    //    RecentTimeMetrics rtm = new RecentTimeMetrics();
    //    System.out.println(rtm.evaluate());
    //}


}
