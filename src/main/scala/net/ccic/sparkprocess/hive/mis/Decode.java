package net.ccic.sparkprocess.hive.mis;

import org.apache.hadoop.hive.ql.exec.UDF;

/**
 * Created by FrankSen on 2018/11/2.
 */
public class Decode extends UDF{


    public static void main(String[] args){

        Decode decode = new Decode();
        System.out.println(decode.evaluate(1.525,1.525,312.155,10.100));

    }


    public String evaluate(Object... args) {
        if (args.length % 2 != 0) {
            throw new RuntimeException("输入的参数个数错误，应为偶数");
        }
        int number = args.length;
        Object result = null;
        int flag = number - 1;

        int i = 1;

        if (args[0] == null) {

           return String.valueOf(args[args.length-1]) ;

        } else {

            if (args[0] instanceof Integer) {
                args[0] = Double.valueOf(Integer.valueOf(args[0].toString()));
            }
            while (i < flag) {

                if (args[i] instanceof Integer) {
                    args[i] = Double.valueOf(Integer.valueOf(args[i].toString()));
                }

                if (String.valueOf(args[i]).equals(String.valueOf(args[0]))) {
                    result = args[i + 1];
                    break;
                } else {
                    i += 2;
                }
            }
            if (result == null)
                result = args[flag];

            return String.valueOf(result);
        }
        }



}
