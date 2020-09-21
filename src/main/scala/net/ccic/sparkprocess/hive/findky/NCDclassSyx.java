package net.ccic.sparkprocess.hive.findky;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

/**
 * Created by FrankSen on 2019/1/23
 */
public class NCDclassSyx extends UDF {

//    public static void main(String[] args){
//        String str = "B47";
//        Text text = new Text(str);
//
//        NCDclassSyx ncd = new NCDclassSyx();
//
//        //System.out.println(ncd.evaluate(null,"DDA","厦门",null,"转保",
//        //        2280.82,null,"","旧车","含车损险"));
//        //System.out.println(ncd.evaluate(null,"DDG","深圳",null,"转保",
//        //        1106.13,1.0,"04","旧车","含车损险"));
//        System.out.println(ncd.evaluate(text,"DEA","北京",1,"续保",
//                3218.02,1.0,"B4","旧车","其他"));
//        //System.out.println(ncd.evaluate(null,"DDK","上海",null,"续保",
//        //        null,null,null,null,null));
//
//    }


    /**
     * 函数名称：             f_get_ncdsyx
     * 作者：                Wangys
     * @param profitratereason
     * @param riskcode             is not null
     * @param dpt                  is not null
     * @param prior_count
     * @param renewal              is not null
     * @param premium_syx
     * @param profitrate
     * @param nonadjustreasoncode1 is not null
     * @param car_new              is not null
     * @param line_class           is not null
     * @return
     */



    public String evaluate(Text profitratereason, String riskcode, String dpt, Integer prior_count
            , String renewal, Double premium_syx, Double profitrate, String nonadjustreasoncode1
            , String car_new, String line_class ){

        String ncdclassone= getNcdClassSyxOne(profitratereason,riskcode,dpt,prior_count,renewal,premium_syx,profitrate);
        return getNcdClassSyxSend(ncdclassone,nonadjustreasoncode1,car_new,line_class);

    }

    /**
     *
     * @param ncdclassone
     * @param nonadjustreasoncode1
     * @param car_new
     * @param line_class
     * @return
     */

    String getNcdClassSyxSend(String ncdclassone, String nonadjustreasoncode1, String car_new, String line_class) {
        // 定义中间结果变量
        String result1 = null;
        //String result2 = null;
        //String result3 = null;
        //String result4 = null;
        // step1
        if (ncdclassone.equals("Unknown")) {
            result1 = "新保或首年投保";
        } else {
            result1 = ncdclassone;
        }
        // step2
        if (nonadjustreasoncode1.equals("01") || nonadjustreasoncode1.equals("06")) {
            result1 = "过户车";
        } else if (nonadjustreasoncode1.equals("02") || nonadjustreasoncode1.equals("08")) {
            result1 = "脱保车";
        }
        // step3
        if (result1.equals("新保或首年投保")) {
            if (car_new.equals("新车")) {
                result1 = "新车";
            } else if (car_new.equals("旧车")) {
                result1 = "首年投保";
            } else {
                result1 = "首年投保";
            }
        }

        // step4
        if (line_class.equals("单保交强险")) {
            result1 = "其他";
        }
        return result1;
    }

    /**
     *
     * @param profitratereason
     * @param riskcode
     * @param dpt
     * @param prior_count
     * @param renewal
     * @param premium_syx
     * @param profitrate
     * @return
     */

    private String getNcdClassSyxOne(Text profitratereason, String riskcode, String dpt,
                                     Integer prior_count, String renewal, Double premium_syx,
                                     Double profitrate) {
        String[] riskcodeList = {"DDC","DDG","DEA","DEB","DFA","DFB"};
        String[] dpt1 = {"厦门","北京"};
        String[] dpt2 = {"深圳","厦门","北京"};
        String[] profitres1 = {"B40","B41"};
        String[] profitres2 = {"B42","B43","B44"};
        String[] profitres3 = {"B51","B52","B53","B54","B55","B56","B57"};
        String[] profitres4 = {"B01","B02"};
        String[] profitres5 = {"B13","B14","B15"};
        String[] profitres6 = {"A1","A2","A3"};
        String[] profitres7 = {"A9","A10","A11","A12"};

        if(!(profitratereason == null)){


            return getProfitIsnotNull(profitratereason, riskcode, dpt, prior_count,
                    riskcodeList, dpt1, dpt2, profitres1, profitres2,
                    profitres3, profitres4, profitres5, profitres6, profitres7);

            //profitratereason is null
        }else{

            return getProfitIsNullString(riskcode, dpt, prior_count, renewal,
                    premium_syx, profitrate, riskcodeList, dpt2);


        }

        //return res;
    }

    private String getProfitIsNullString(String riskcode, String dpt, Integer prior_count, String renewal,
                                         Double premium_syx, Double profitrate, String[] riskcodeList, String[] dpt2) {
        String res = null;
        if(renewal.equals("新保")){
            res = "新车";

        }else{
            if(premium_syx == null || premium_syx == 0 ){
                res = "Unknown";

            }else if(premium_syx !=0 && premium_syx != null){
                //profitrate == null
                if(profitrate == null){
                    if(prior_count != null){
                        if(prior_count == 0 ){
                            res = "上年没有发生赔款";

                        }else if(prior_count==1){
                            res = "上年发生1次赔款";

                        }else if(prior_count==2){
                            res = "上年发生2次赔款";

                        }else if(prior_count==3){
                            res = "上年发生3次赔款";

                        }else if(prior_count==4){
                            res = "上年发生4次赔款";

                        }else if(prior_count>5){
                            res = "上年发生5次及以上赔款";
                        }else{
                            res = "Unknown";
                        }
                    }else{
                        res = "Unknown";
                    }


                    //    profitrate != null
                }else{
                    if(useArrBinSed(riskcodeList,riskcode)){
                        if(profitrate < 0.605){

                            res = "连续3年没有发生赔款";
                        } else if (profitrate > 0.605 && profitrate < 0.705) {
                            res = "连续2年没有发生赔款";

                        }else if (profitrate > 0.705 && profitrate < 0.855) {
                            res = "上年没有发生赔款";

                        }else if (profitrate > 0.855 && profitrate < 1.005) {
                            if (prior_count == null ) {
                                res = "新保或首年投保";
                            }else if (prior_count == 0){
                                res = "新保或首年投保";
                            }else {

                                res = "上年发生1次赔款";
                            }

                        }else if(profitrate > 1.005 && profitrate < 1.255){
                            res = "上年发生2次赔款";
                        }else if(profitrate > 1.255 && profitrate < 1.505){
                            res = "上年发生3次赔款";
                        }else if(profitrate > 1.505 && profitrate < 1.755){
                            res = "上年发生4次赔款";
                        }else if(profitrate > 1.755){
                            res = "上年发生5次及以上赔款";
                        }else {
                            res = "Unknown";
                        }
                    }else if(!useArrBinSed(dpt2,dpt)){

                        if(profitrate < 0.705){

                            res = "连续3年没有发生赔款";
                        }else if(profitrate > 0.705 && profitrate < 0.805){
                            res = "连续2年没有发生赔款";
                        }else if(profitrate > 0.805 && profitrate < 0.905){
                            res = "上年没有发生赔款";
                        }else if(profitrate > 0.905 && profitrate < 1.005){
                            res = getPriorCountString(prior_count);
                        }else if(profitrate > 1.005 && profitrate < 1.105){
                            res = "上年发生3次赔款";
                        }else if(profitrate > 1.105 && profitrate < 1.205){
                            res = "上年发生4次赔款";
                        }else if(profitrate > 1.205){
                            res = "上年发生5次及以上赔款";
                        }

                    }else if(dpt.equals("深圳")){

                        if(profitrate < 0.505){

                            res =  "连续3年没有发生赔款";
                        }else if(profitrate > 0.505 && profitrate < 0.555){
                            res =  "连续2年没有发生赔款";
                        }else if(profitrate > 0.555 && profitrate < 0.605){
                            res =  "上年没有发生赔款";
                        }else if(profitrate > 0.605 && profitrate < 0.705){
                            res =  "上年发生1次赔款";

                        }else if(profitrate > 0.905 && profitrate < 1.005){
                            if(prior_count == null){

                                res =  "新保或首年投保";
                            }else if(prior_count == 2){

                                res =  "上年发生2次赔款";
                            }else{
                                res =  "新保或首年投保";
                            }

                        }else if(profitrate > 1.005 && profitrate < 1.105){
                            res =  "上年发生3次赔款";

                        }else if(profitrate > 1.105 && profitrate < 1.305){
                            res =  "上年发生4次赔款";
                        }else if(profitrate > 1.305){
                            res =  "上年发生5次及以上赔款";
                        }

                    }else if(dpt.equals("厦门")){

                        if(profitrate < 0.505){

                            res =  "连续3年没有发生赔款";
                        }else if(profitrate > 0.505 && profitrate < 0.605){
                            res =  "连续2年没有发生赔款";
                        }else if(profitrate > 0.605 && profitrate < 0.705){
                            res =  "上年没有发生赔款";
                        }else if(profitrate > 0.605 && profitrate < 0.705){
                            res =  "上年发生1次赔款";

                        }else if(profitrate > 0.905 && profitrate < 1.005){
                            res = getPriorCount2String(prior_count);

                        }else if(profitrate > 1.005 && profitrate < 1.055){
                            res =  "上年发生2次赔款";

                        }else if(profitrate > 1.055 && profitrate < 1.105){
                            res =  "上年发生3次赔款";
                        }else if(profitrate > 1.105 && profitrate < 1.305){
                            res =  "上年发生4次赔款";
                        }else if(profitrate > 1.305){
                            res =  "上年发生5次及以上赔款";
                        }

                    }else if(dpt.equals("北京")){

                        if(profitrate <= 0.6){

                            res =  "连续3年没有发生赔款";
                        }else if(profitrate > 0.6 && profitrate <= 0.705){
                            res =  "连续2年没有发生赔款";
                        }else if(profitrate == 0.85){
                            if(prior_count == null){
                                res =  "上年发生2次赔款";

                            }else if(prior_count == 0){
                                res =  "上年没有发生赔款";

                            }else if(prior_count == 1){
                                res =  "上年发生1次赔款";

                            }else {
                                res =  "上年发生2次赔款";

                            }
                        }else if(profitrate == 0.9 || profitrate ==1 ){
                            res =  getPriorCountString(prior_count);

                        }else if(profitrate == 0.99 || profitrate == 1.1){
                            res =  "上年发生3次赔款";

                        }else if( profitrate == 1.08 || profitrate == 1.2){
                            res =  "上年发生4次赔款";

                        }else if(profitrate >= 1.35){
                            res =  "上年发生5次及以上赔款";
                        }else if(profitrate > 1.105 && profitrate < 1.305){
                            res =  "上年发生4次赔款";
                        }else if(profitrate > 1.305){
                            res =  "上年发生5次及以上赔款";
                        }

                    }

                }


            }else{
                res =  null;
            }
        }
        return res;
    }

    private String getPriorCount2String(Integer prior_count) {
        String res;
        if(prior_count == null){

            res =  "新保或首年投保";
        }else if(prior_count == 1){

            res =  "上年发生1次赔款";
        }else{
            res =  "新保或首年投保";
        }
        return res;
    }

    /**
     *
     * @param profitratereason
     * @param riskcode
     * @param dpt
     * @param prior_count
     * @param riskcodeList
     * @param dpt1
     * @param dpt2
     * @param profitres1
     * @param profitres2
     * @param profitres3
     * @param profitres4
     * @param profitres5
     * @param profitres6
     * @param profitres7
     * @return
     */

    private String getProfitIsnotNull(Text profitratereason, String riskcode, String dpt, Integer prior_count
            , String[] riskcodeList, String[] dpt1, String[] dpt2, String[] profitres1
            , String[] profitres2, String[] profitres3, String[] profitres4, String[] profitres5
            , String[] profitres6, String[] profitres7) {

        String res = null;

        //riskcode in('DDC','DDG','DEA','DEB','DFA','DFB')
        if(useArrBinSed(riskcodeList, riskcode) ){
            //dpt in('厦门','北京')

            if( useArrBinSed(dpt1,dpt)){

                if(useArrBinSed(profitres1,profitratereason.toString())){
                    res = "新保或首年投保";
                }else if(useArrBinSed(profitres2,profitratereason.toString())){
                    res = "连续3年没有发生赔款";

                }else if(profitratereason.toString().equals("B45")){
                    res = "连续2年没有发生赔款";

                }else if(profitratereason.toString().equals("B46")){
                    res = "上年没有发生赔款";

                }else if(profitratereason.toString().equals("B47")){
                    res = "上年发生1次赔款";

                }else if(profitratereason.toString().equals("B48")){
                    res = "上年发生2次赔款";

                }else if(profitratereason.toString().equals("B49")){
                    res = "上年发生3次赔款";

                }else if(profitratereason.toString().equals("B50")){
                    res = "上年发生4次赔款";

                }else if(useArrBinSed(profitres3,profitratereason.toString())){
                    res = "上年发生5次及以上赔款";

                }else{
                    res = "Unknown";

                }
                //dpt not in('厦门','北京'）
            }else{

                if(profitratereason.toString().equals("B31")){

                    res = getPriorCount2String(prior_count);

                }else if(profitratereason.toString().equals("B13")){
                    res = "连续3年没有发生赔款";

                }else if(profitratereason.toString().equals("B12")){
                    res = "连续2年没有发生赔款";

                }else if(profitratereason.toString().equals("B11")){
                    res = "上年没有发生赔款";

                }else if(profitratereason.toString().equals("B32")){
                    res = "上年发生2次赔款";

                }else if(profitratereason.toString().equals("B33")){
                    res = "上年发生3次赔款";

                }else if(profitratereason.toString().equals("B34")){
                    res = "上年发生4次赔款";

                }else if(profitratereason.toString().equals("B35")){
                    res = "上年发生5次及以上赔款";

                }else{
                    res = "Unknown";
                }

            }

            //  dpt not in ("深圳","厦门","北京")

        }else if(!useArrBinSed(dpt2,dpt)){

            if(profitratereason.toString().equals("B4")){
                if(prior_count == null ) {
                    res =  "新保或首年投保";
                }else if(prior_count ==1) {
                    res =  "上年发生1次赔款";
                }else if(prior_count ==2) {
                    res =  "上年发生2次赔款";
                }else {
                    res =  "新保或首年投保";
                }

            }else{
                if(profitratereason.toString().equals("B1")){
                    res =  "连续3年没有发生赔款";

                }else if(profitratereason.toString().equals("B2")){
                    res =  "连续2年没有发生赔款";
                }else if(profitratereason.toString().equals("B3")){
                    res =  "上年没有发生赔款";
                }else if(profitratereason.toString().equals("B4")){
                    res =  "上年发生3次赔款";
                }else if(profitratereason.toString().equals("B5")){
                    res =  "上年发生4次赔款";
                }else if(profitratereason.toString().equals("B7")){
                    res =  "上年发生5次以上赔款";
                }else{
                    res =  "Unknown";
                }

            }

            //  dpt in ('深圳')
        }else if(dpt.equals("深圳")){
            if(profitratereason.toString().equals("A1")){
                res =  "连续3年没有发生赔款";
            }else if(profitratereason.toString().equals("A2")){
                res =  "连续2年没有发生赔款";

            }else if(profitratereason.toString().equals("A3")){
                res =  "上年没有发生赔款";

            }else if(profitratereason.toString().equals("A4")){
                res =  "上年发生1次赔款";

            }else if(profitratereason.toString().equals("A5")){
                res =  "上年发生2次赔款";

            }else if(profitratereason.toString().equals("A6")){
                res =  "上年发生3次赔款";

            }else if(profitratereason.toString().equals("A7")){
                res =  "上年发生4次赔款";

            }else if(profitratereason.toString().equals("A11")){
                res =  "新保或首年投保";

            }else {
                res =  "上年发生5次及以上赔款";

            }

            //  dpt in ('厦门')
        }else if(dpt.equals("厦门")){
            if(useArrBinSed(profitres4,profitratereason.toString())){
                res =  "新保或首年投保";
            }else if(useArrBinSed(profitres5,profitratereason.toString())){
                res =  "连续3年没有发生赔款";

            }else if(profitratereason.toString().equals("B12")){
                res =  "连续2年没有发生赔款";

            }else if(profitratereason.toString().equals("B11")){
                res =  "上年没有发生赔款";

            }else if(profitratereason.toString().equals("B31")){
                res =  "上年发生1次赔款";

            }else if(profitratereason.toString().equals("B32")){
                res =  "上年发生2次赔款";

            }else if(profitratereason.toString().equals("B33")){
                res =  "上年发生3次赔款";

            }else if(profitratereason.toString().equals("B34")){
                res =  "上年发生4次赔款";

            }else {
                res =  "上年发生5次及以上赔款";

            }
            //  dpt in ('北京')
        }else if(dpt.equals("北京")){
            if(profitratereason.toString().equals("A13") || profitratereason.toString().equals("A14")){
                res =  "新保或首年投保";
            }else if(useArrBinSed(profitres6, profitratereason.toString())){
                res =  "连续3年没有发生赔款";

            }else if(profitratereason.toString().equals("A4")){
                res =  "连续2年没有发生赔款";

            }else if(profitratereason.toString().equals("A5")){
                res =  "上年没有发生赔款";

            }else if(profitratereason.toString().equals("A7")){
                res =  "上年发生3次赔款";

            }else if(profitratereason.toString().equals("A8")){
                res =  "上年发生4次赔款";

            }else if(useArrBinSed(profitres7,profitratereason.toString())){
                res =  "发生5次及以上赔款";

            }else {
                if(prior_count == null){

                    res =  "上年发生2次赔款";
                }else if(prior_count == 1){
                    res =  "上年发生1次赔款";
                }else{
                    res =  "上年发生2次赔款";
                }

            }

        }
        return res;
    }

    /**
     *
     * @param prior_count
     * @return
     */

    private String getPriorCountString(Integer prior_count) {
        if(prior_count == null ){

            return "新保或首年投保";
        }else if(prior_count == 0){

            return "新保或首年投保";
        }else if(prior_count == 1){

            return "上年发生1次赔款";
        }else {
            return "上年发生2次赔款";
        }
    }

    /**
     *
     * @param riskcodeList
     * @param value
     * @return
     */
    private boolean useArrBinSed(String[] riskcodeList,String value) {
        //String[] riskcodeList = {"DDC","DDG","DEA","DEB","DFA","DFB"};
        int a = findPositionFromArray(riskcodeList, value);
        if (a >= 0)
            return true;
        else
            return false;

    }

    /**
     *
     * @param arr
     * @param targetValue
     * @return
     */

    public static int findPositionFromArray(String[] arr, String targetValue){
        return ArrayUtils.indexOf(arr, targetValue);
    }



}
