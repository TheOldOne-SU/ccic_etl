package net.ccic.sparkprocess.hive.mis;

import org.apache.hadoop.hive.ql.exec.UDF;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by FrankSen on 2018/8/12.
 */
public class GetValidBirthDay extends UDF {

    public String evaluate(String funName, String id){
        if (funName.equals("id")){
            return getBirthday(id);
        }else if (funName.equals("sex")){
            return getSex(id);

        }else{
            return "";
        }





    }

    public String getSex(String id){
        if (id != null) {
            int idLength = id.length();
            if(isCardNum(id) && idLength == 18){
                int d = Integer.parseInt(id.substring(16, 17))%2;
                if (d == 0){
                    return "女";
                }else {
                    return "男";
                }
            }else if (isCardNum(id) && idLength == 15){
                int d = Integer.parseInt(id.substring(14, 15))%2;
                if (d == 0){
                    return "女";
                }else {
                    return "男";
                }

            }else {
                return "未知";
            }
        }else{
            return "未知";
        }
    }

    public String getBirthday(String id){

        if (id != null){
            int idLength = id.length();
            StringBuffer sb = new StringBuffer();

            if (isCardNum(id) && idLength == 18){
                sb.append(id.substring(6, 10));
                sb.append("-");
                sb.append(id.substring(10, 12));
                sb.append("-");
                sb.append(id.substring(12, 14));

                return sb.toString();

            }else if(isCardNum(id) && idLength == 15){
                sb.append("19");
                sb.append(id.substring(6, 8));
                sb.append("-");
                sb.append(id.substring(8, 10));
                sb.append("-");
                sb.append(id.substring(10, 12));

                return sb.toString();

            }else{

                return "1900-01-03";
            }
        }else{
            return "1900-01-03";
        }
    }

    public static boolean isCardNum(String id){
        String regex = "(^[1-8]\\d{5}(18|19|20)?(\\d{2})([01]\\d)([0123]\\d)(\\d{3})(\\d|X)?$)" +
                "|(^[1-8]\\d{7}((0\\d)|(1[0-2]))(([0|1|2]\\d)|3[0-1])\\d{3}$)";
        return matcher(regex,id) ;
    }
    private static boolean matcher(String regex, String str){
        Pattern pattern = Pattern.compile(regex);
        Matcher matcher = pattern.matcher(str);
        return matcher.matches();
    }

    //public static void main(String[] args){
    //    GetValidBirthDay g = new GetValidBirthDay();
    //    System.out.println(g.evaluate("sex","411522199310230316"));
    //
    //}

}
