package net.ccic.sparkprocess.hive.mis;

import org.apache.hadoop.hive.ql.exec.UDF;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;


/**
 * Created by FrankSen on 2018/7/3.
 */
public class GetEarned extends UDF {

    //创建SimpleDataFormat实例对象
    SimpleDateFormat sdf = new SimpleDateFormat ("yyyy-MM-dd");
    //创建Calendar-日历对象
    Calendar c = Calendar.getInstance();
    //设置保留几位小数
    //DecimalFormat df = new DecimalFormat("0.0000000000000000");



    /**
     *函数名称：             f_get_earnedearned
     * 作者：                Wangys
     * @param p_begindate   查询起始日期
     * @param p_enddate     查询终止日期
     * @param v_earnedtype  满期参数 pl  保单累计满期 py 保单本年满期 pm 保单本月满期
     *                              el  批单累计满期 ey 批单本年满期 em 批单本月满期
     * @param v_startdate   起保日期
     * @param v_validdate   生效日期
     * @param v_enddate     终保日期
     * @param v_uwdate      核保核批日期
     * @param v_edrtype     批改类型
     * @return
     */


    public  Double evaluate (String p_begindate, String p_enddate, String v_startdate, String v_enddate,
                           String v_validdate, String v_earnedtype, String v_uwdate, String v_edrtype) throws Exception {




        if(v_earnedtype == null){
            return 100.0;
        }else {
            if(v_earnedtype.equals("pl")){
                return getPl(v_startdate, v_enddate, p_enddate);

            }else if(v_earnedtype.equals("py")){
                return getPy(v_startdate, v_enddate, p_begindate, p_enddate, v_uwdate);

            }else if(v_earnedtype.equals("pm")){
                return getPm(v_startdate, v_enddate, p_begindate, p_enddate, v_uwdate);

            }else if(v_earnedtype.equals("el")){
                return getEl(v_startdate, v_enddate, v_edrtype, v_validdate, p_enddate);

            }else if(v_earnedtype.equals("em")){
                return getEm(v_startdate, v_enddate, v_edrtype, v_uwdate, v_validdate, p_begindate, p_enddate);

            }else if(v_earnedtype.equals("ey")){
                return getEy(v_startdate, v_enddate, v_uwdate, v_edrtype, v_validdate, p_begindate, p_enddate);

            }else {
                return 0.0;
            }
        }
    }

    /**
     *
     * @param v_startdate
     * @param v_enddate
     * @param p_enddate
     * @return
     */
    private Double getPl(String v_startdate, String v_enddate, String p_enddate) throws Exception {
        if(dateTransLongType(v_startdate) <= dateTransLongType(v_enddate)){
            return transDoubleFormat(diff_date(least(v_enddate, p_enddate), v_startdate) + 1, diff_date(v_enddate, v_startdate) + 1);
        }else{
            return 1.0;

        }
    }


    /**
     *
     * @param v_startdate
     * @param v_enddate
     * @param p_begindate
     * @param p_enddate
     * @param v_uwdate
     * @return
     */

    private Double getPy(String v_startdate, String v_enddate, String p_begindate, String p_enddate, String v_uwdate) throws Exception {
        if(dateTransLongType(v_startdate) > dateTransLongType(v_enddate)){
            return 1.0;
        }else if(dateTransLongType(v_uwdate) <= dateTransLongType(v_startdate)){

            return transDoubleFormat(diff_date(least(v_enddate, p_enddate), greatest(v_startdate, trunc(p_begindate))) + 1, diff_date(v_enddate, v_startdate) + 1);

        }else if(dateTransLongType(v_uwdate) > dateTransLongType(v_startdate) && dateTransLongType(p_enddate) >= dateTransLongType(v_enddate) && dateTransLongType(v_uwdate) >= dateTransLongType(trunc(p_begindate))){

            return transDoubleFormat(diff_date(v_enddate, v_startdate) + 1, diff_date(v_enddate, v_startdate) + 1);

        }else if(dateTransLongType(v_uwdate) > dateTransLongType(v_startdate) && dateTransLongType(p_enddate) >= dateTransLongType(v_enddate) && dateTransLongType(v_uwdate) < dateTransLongType(trunc(p_begindate))){

            return transDoubleFormat(diff_date(v_enddate, trunc(p_begindate)) + 1, diff_date(v_enddate, v_startdate) + 1);

        }else if(dateTransLongType(v_uwdate) > dateTransLongType(v_startdate) && dateTransLongType(p_enddate) < dateTransLongType(v_enddate) && dateTransLongType(v_uwdate) >= dateTransLongType(trunc(p_begindate))){

            return transDoubleFormat(diff_date(p_enddate, v_startdate) + 1, diff_date(v_enddate, v_startdate) + 1);

        }else if(dateTransLongType(v_uwdate) > dateTransLongType(v_startdate) && dateTransLongType(p_enddate) < dateTransLongType(v_enddate) && dateTransLongType(v_uwdate) < dateTransLongType(trunc(p_begindate))){

            return transDoubleFormat(diff_date(p_enddate, trunc(p_begindate)) + 1, diff_date(v_enddate, v_startdate) + 1);

        }else{
            return 0.0;
        }
    }



    /**
     *
     * @param v_startdate
     * @param v_enddate
     * @param p_begindate
     * @param p_enddate
     * @param v_uwdate
     * @return
     */

    private Double getPm(String v_startdate, String v_enddate, String p_begindate, String p_enddate, String v_uwdate) throws Exception {
        if(dateTransLongType(v_startdate) >= dateTransLongType(p_begindate) || dateTransLongType(v_enddate) >= dateTransLongType(p_begindate) || dateTransLongType(v_uwdate) >= dateTransLongType(p_begindate)){
            if(dateTransLongType(v_startdate) > dateTransLongType(v_enddate)){
                return 1.0;
            }else if(dateTransLongType(v_uwdate) <= dateTransLongType(v_startdate)){

                return transDoubleFormat(diff_date(least(v_enddate, p_enddate), greatest(v_startdate, p_begindate)) + 1, diff_date(v_enddate, v_startdate) + 1);

            }else if(dateTransLongType(v_uwdate) > dateTransLongType(v_startdate) && dateTransLongType(p_enddate) >= dateTransLongType(v_enddate) && dateTransLongType(v_uwdate) >= dateTransLongType(p_begindate)){

                return transDoubleFormat(diff_date(v_enddate, v_startdate) + 1, diff_date(v_enddate, v_startdate) + 1);

            }else if(dateTransLongType(v_uwdate) > dateTransLongType(v_startdate) && dateTransLongType(p_enddate) >= dateTransLongType(v_enddate) && dateTransLongType(v_uwdate) < dateTransLongType(p_begindate)){

                return transDoubleFormat(diff_date(v_enddate, p_begindate) + 1, diff_date(v_enddate, v_startdate) + 1);

            }else if(dateTransLongType(v_uwdate) > dateTransLongType(v_startdate) && dateTransLongType(p_enddate) < dateTransLongType(v_enddate) && dateTransLongType(v_uwdate) >= dateTransLongType(p_begindate)){

                return transDoubleFormat(diff_date(p_enddate, v_startdate) + 1, diff_date(v_enddate, v_startdate)+ 1);

            }else if(dateTransLongType(v_uwdate) > dateTransLongType(v_startdate) && dateTransLongType(p_enddate) < dateTransLongType(v_enddate) && dateTransLongType(v_uwdate) < dateTransLongType(p_begindate)){

                return transDoubleFormat(diff_date(p_enddate, p_begindate) + 1, diff_date(v_enddate, v_startdate) + 1);

            }else {
                return 0.0;
            }
        }else {
            return 0.0;
        }
    }

    /**
     *
     * @param v_startdate
     * @param v_enddate
     * @param v_edrtype
     * @param v_validdate
     * @param p_enddate
     * @return
     */
    private Double getEl(String v_startdate, String v_enddate, String v_edrtype, String v_validdate, String p_enddate) throws Exception {
        String substrRtnDate = returnSubstr(v_edrtype, v_startdate, v_validdate);
        if(dateTransLongType(substrRtnDate) <= dateTransLongType(v_enddate)){

            return transDoubleFormat(diff_date(least(v_enddate, p_enddate), substrRtnDate) + 1, diff_date(v_enddate, substrRtnDate) + 1);

        }else {
            return 1.0;
        }
    }


    /**
     *
     * @param v_startdate
     * @param v_enddate
     * @param v_edrtype
     * @param v_uwdate
     * @param v_validdate
     * @param p_begindate
     * @param p_enddate
     * @return
     */
    private Double getEm(String v_startdate, String v_enddate, String v_edrtype, String v_uwdate, String v_validdate, String p_begindate, String p_enddate) throws Exception {
        String substrRtnDate = returnSubstr(v_edrtype, v_startdate, v_validdate);

        //System.out.println("-------xxxxxxxxxxxxxx-------------");
        //System.out.println(substrRtnDate);

        if(dateTransLongType(substrRtnDate) >= dateTransLongType(p_begindate) || dateTransLongType(v_enddate) >= dateTransLongType(p_begindate) || dateTransLongType(v_uwdate) >= dateTransLongType(p_begindate)){
            if(dateTransLongType(substrRtnDate) > dateTransLongType(v_enddate)){
                return 1.0;
            }else if(dateTransLongType(v_uwdate) <= dateTransLongType(substrRtnDate) && dateTransLongType(substrRtnDate) > dateTransLongType(p_enddate)){

                return 0.0;

            }else if(dateTransLongType(v_uwdate) <= dateTransLongType(substrRtnDate)){

                return transDoubleFormat( diff_date(least(v_enddate, p_enddate), greatest(substrRtnDate, p_begindate)) + 1, diff_date(v_enddate, substrRtnDate) + 1);

            }else if(dateTransLongType(v_uwdate) > dateTransLongType(substrRtnDate) && dateTransLongType(p_enddate) >= dateTransLongType(v_enddate) && dateTransLongType(v_uwdate) >= dateTransLongType(p_begindate)){

                return transDoubleFormat(diff_date(v_enddate, substrRtnDate) + 1, diff_date(v_enddate, substrRtnDate) + 1);

            }else if(dateTransLongType(v_uwdate) > dateTransLongType(substrRtnDate) && dateTransLongType(p_enddate) >= dateTransLongType(v_enddate) && dateTransLongType(v_uwdate) < dateTransLongType(p_begindate)){

                return transDoubleFormat(diff_date(v_enddate,p_begindate) + 1, diff_date(v_enddate, substrRtnDate) + 1);

            }else if(dateTransLongType(v_uwdate) > dateTransLongType(substrRtnDate) && dateTransLongType(p_enddate) < dateTransLongType(v_enddate) && dateTransLongType(v_uwdate) >= dateTransLongType(p_begindate)){

                return transDoubleFormat(diff_date(p_enddate, substrRtnDate)+1, diff_date(v_enddate, substrRtnDate) +1);

            }else if(dateTransLongType(v_uwdate) > dateTransLongType(substrRtnDate) && dateTransLongType(p_enddate) < dateTransLongType(v_enddate) && dateTransLongType(v_uwdate) < dateTransLongType(p_begindate)){

                //System.out.println("-------xxxxxxxxxxxxxx-------------");
                //System.out.println(diff_date(p_enddate, p_begindate)+1);
                //System.out.println(diff_date(v_enddate, substrRtnDate) + 1);

                return transDoubleFormat(diff_date(p_enddate, p_begindate) + 1, diff_date(v_enddate, substrRtnDate) + 1);

            }else {
                return 0.00;
            }

        }else {
            return 0.0;
        }

    }

    /**
     *
     * @param v_startdate
     * @param v_enddate
     * @param v_uwdate
     * @param v_edrtype
     * @param v_validdate
     * @param p_begindate
     * @param p_enddate
     * @return
     */
    private Double getEy(String v_startdate, String v_enddate, String v_uwdate, String v_edrtype, String v_validdate, String p_begindate, String p_enddate) throws Exception {
        String substrRtnDate = returnSubstr(v_edrtype, v_startdate, v_validdate);

        if(dateTransLongType(substrRtnDate) > dateTransLongType(v_enddate)){

            return 1.0;

        }else if(dateTransLongType(v_uwdate) <= dateTransLongType(substrRtnDate) && dateTransLongType(substrRtnDate) > dateTransLongType(p_enddate)){

            return 0.0;

        }else if(dateTransLongType(v_uwdate) <= dateTransLongType(substrRtnDate)){

            return transDoubleFormat(diff_date(least(v_enddate, p_enddate), greatest(substrRtnDate, p_begindate)) + 1, diff_date(v_enddate, substrRtnDate) + 1);

        }else if(dateTransLongType(v_uwdate) > dateTransLongType(substrRtnDate) && dateTransLongType(p_enddate) >= dateTransLongType(v_enddate) && dateTransLongType(v_uwdate) >= dateTransLongType(trunc(p_begindate))){

            return transDoubleFormat(diff_date(v_enddate, substrRtnDate) + 1, diff_date(v_enddate, substrRtnDate) + 1);

        }else if (dateTransLongType(v_uwdate) > dateTransLongType(substrRtnDate) && dateTransLongType(p_enddate) >= dateTransLongType(v_enddate) && dateTransLongType(v_uwdate) < dateTransLongType(trunc(p_begindate))){

            return transDoubleFormat(diff_date(v_enddate, trunc(p_begindate)) + 1, diff_date(v_enddate, substrRtnDate) + 1);
        }else if(dateTransLongType(v_uwdate) > dateTransLongType(substrRtnDate) && dateTransLongType(p_enddate) < dateTransLongType(v_enddate) && dateTransLongType(v_uwdate) >= dateTransLongType(trunc(p_begindate))){

            return transDoubleFormat(diff_date(p_enddate, substrRtnDate) + 1, diff_date(v_enddate, substrRtnDate) + 1);

        }else if(dateTransLongType(v_uwdate) > dateTransLongType(substrRtnDate) && dateTransLongType(p_enddate) < dateTransLongType(v_enddate) && dateTransLongType(v_uwdate) < dateTransLongType(trunc(p_begindate))){

            return transDoubleFormat(diff_date(p_enddate, trunc(p_begindate)) + 1, diff_date(v_enddate, substrRtnDate) + 1);

        }else {
            return 0.00;
        }

    }

    /**
     *
     * @param divisor
     * @param dividend
     * @return
     */

    /**
     * 截取 v_edrtype 字段，根据条件返回指定字段
     * @param v_edrtype
     * @param v_startdate
     * @param v_validdate
     * @return
     */

    private String returnSubstr(String v_edrtype, String v_startdate, String v_validdate) {
        if (v_edrtype == null || v_edrtype.equals("")){
            return v_validdate;
        }else{
            if(v_edrtype.trim().length() >= 2){
                if(v_edrtype.trim().substring(0, 2).equals("19")){

                    return v_startdate;
                }else{
                    return v_validdate;
                }
            }else{
                return v_validdate;
            }
        }
    }

    private Double transDoubleFormat(int divisor, int dividend){
        // 分母为0 时返会 0.0
        if(dividend == 0){
            return  0.0;
        }else {
            return (double)divisor / dividend;
        }
    }


    /**
     * 时期间隔
     * @param date1
     * @param date2
     * @return
     * @throws Exception
     */
    private int diff_date(String date1, String date2) throws Exception {
        long b = sdf.parse(date1).getTime() - sdf.parse(date2).getTime();
        int num = (int) (b / (1000 * 3600 * 24));
        return num;
    }

    /**
     * 日期转换
     * @param date
     * @return
     * @throws ParseException
     */

    public long dateTransLongType(String date) throws ParseException {
        return sdf.parse(date).getTime();
    }

    /**
     * 日期加法运算
     * @param nowDate
     * @param amount
     * @return
     */

    private String dateSum(String nowDate, int amount) throws ParseException {
        c.setTime(sdf.parse(nowDate));
        c.add(Calendar.DAY_OF_MONTH, amount);
        return sdf.format(c.getTime());
    }

    /**
     * 计算最小日期
     * @param date1
     * @param date2
     * @return
     */
    private String least(String date1, String date2) throws ParseException {
        if(dateTransLongType(date1) < dateTransLongType(date2)){
            return date1;
        }else {
            return date2;
        }
    }

    /**
     * 比较最大日期
     * @param date1
     * @param date2
     * @return
     * @throws ParseException
     */

    private  String greatest(String date1, String date2) throws ParseException {
        if(dateTransLongType(date1) > dateTransLongType(date2)){
            return date1;
        }else {
            return date2;
        }
    }

    /**
     * 如同Oracle <trunc> 函数 获取输入年的第一个日期
     * @param date
     * @return
     * @throws ParseException
     */
    private String trunc(String date) throws ParseException {
        int year = Integer.parseInt(date.substring(0, 4));
        c.clear();
        c.set(Calendar.YEAR, year);
        return sdf.format(c.getTime());
    }

    //public static void main(String[] args) throws Exception {
    //    GetEarned getEarned = new GetEarned();
    //    //SimpleDateFormat sdf = new SimpleDateFormat ("yyyy-MM-dd");
    //////    //int val = getEarned.diff_date(sdf,"2018-07-01 12:00:00", "2018-07-02 11:00:00");
    //////    //System.out.println("相差："+ Math.abs(val) + "天");
    //////    //System.out.println(getEarned.greast("2018-07-01 12:00:00", "2018-07-02 11:00:00"));
    //////
    //////    //System.out.println(getEarned.trunc("2012-07-01 12:00:00"));
    //////    //System.out.println(getEarned.dateSum("2018-07-03", -2));
    //////    //System.out.println(getEarned.dateTransLongType("2018-07-03"));
    //////    //if (getEarned.dateTransLongType("2018-07-03 00:00:00") > getEarned.dateTransLongType("2018-07-01 00:00:00")){
    //////    //
    //////    //    System.out.println(true);
    //////    //}else {
    //////    //    System.out.println(false);
    //////    //}
    //////    // (diff_date(least(v_enddate,p_enddate),greast(v_startdate,trunc(p_begindate)))
    //////    //            / diff_date(v_enddate,dateSum(trunc(v_startdate), 1))) * 1.0
    //////    //System.out.println(getEarned.diff_date(getEarned.greast("2018-07-03 00:00:00","2018-07-01 00:00:00" ),"2018-06-01 00:00:00"));
    //////    //System.out.println(getEarned.diff_date(getEarned.greast("2018-07-03 00:00:00","2018-07-01 00:00:00" ),getEarned.dateSum(getEarned.trunc("2018-06-01 00:00:00"), 1)));
    //    System.out.println(getEarned.getPy("2018-07-03 00:00:00","2019-07-03 00:00:00","2018-06-03 00:00:00","2018-10-03 00:00:00","2018-07-03 00:00:00"));
    //    System.out.println(getEarned.evaluate("2018-06-03 00:00:00","2018-10-03 00:00:00", "2018-07-03 00:00:00","2019-07-03 00:00:00", "2018-07-03 00:00:00", "py","2018-07-03 00:00:00", ""));
    //    System.out.println(getEarned.evaluate("2018-05-01","2018-05-31","2016-09-18 00:00:00.0","2026-05-26 00:00:00.0","2017-05-15 00:00:00.0","em","2017-05-16 00:00:00.0","19"));
    //////    System.out.println(getEarned.evaluate("2018-05-01","2018-05-31","2017-12-26 00:00:00.0","2018-12-25 00:00:00.0","2017-12-29 00:00:00.0","em","2017-12-28 00:00:00.0","17"));
    //////    System.out.println(getEarned.evaluate("2018-05-01","2018-05-31","2018-01-21 00:00:00.0","2019-01-20 00:00:00.0","2018-01-21 00:00:00.0","pm","2018-01-19 00:00:00.0",""));
    ////////
    //////    //System.out.println(getEarned.df.format((float)57/580));
    //}

}
