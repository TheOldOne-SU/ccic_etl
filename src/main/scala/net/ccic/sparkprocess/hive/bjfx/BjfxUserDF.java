package net.ccic.sparkprocess.hive.bjfx;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.hadoop.hive.ql.exec.UDF;

/**
 * Created by FrankSen on 2018/7/11.
 */
public class BjfxUserDF extends UDF {

    public String evaluate(String ... value){
        if (value[0].equals("dpt")){
            return deal_dpt_name(value[1]);
        }else if(value[0].equals("channel")){
            return business_code_channel(value[1]);
        }else if(value[0].equals("identify")){
            return deal_identify_id(value[1], value[2]);
        }else if(value[0].equals("renewyear")) {
            return dealRenewyear(value[1], value[2], value[3]);

        }else{
            return "";
        }
    }

    private String dealRenewyear(String policyno, String policyno_concat, String flag_concat) {
        //System.out.println(policyno_concat+">>>>>>>>>>>>>>>>>");
        //System.out.println(flag_concat+">>>>>>>>>>>>>>>>>");
        if(policyno != null && !policyno.equals("")){
            if(policyno_concat != null){

                String[] policynoArr = policyno_concat.split(",");
                String[] flagArr = flag_concat.split(",");
                int postition = findPositionFromArray(policynoArr, policyno);
                if(postition+1 == flagArr.length){
                    return "1";
                }else {

                    int stmp = 1;
                    for(int i = postition + 1; i <= flagArr.length-1; i++){
                        if(flagArr[i].equals("1")){
                            stmp += 1;
                            if (stmp > 6){
                                break;
                            }
                        }else{
                            break;
                        }

                    }
                    return Integer.toString(stmp);
                }
            }else{
                return "1";
            }

        }else{
            return "0";
        }


    }

    public String deal_identify_id(String identifytype, String identifynumber){

        if (identifytype == null){
            return "非身份证";
        }else {
            if(identifytype.equals("01") || identifytype.equals("111")){

                if (identifynumber != null) {
                    String identify = identifynumber.replaceAll(" ","");
                    if(identify.length() == 15 || identify.length() == 18){
                        String one = identify.substring(0,1);
                        String[] array = {"1", "2", "3", "4", "5", "6"};

                        if(useArrBinSea(array, one)){
                            return "标准身份证";
                        }else{
                            return "非标准身份证";
                        }

                    }else{
                        return "非标准身份证";
                    }

                }else {
                    return "非标准身份证";
                }



            }else {
                return "非身份证";
            }

        }

    }

    public String deal_dpt_name(String comcode){
        if (comcode.equals("") || comcode == null){
            return "信息缺失";
        }else{
            String comcodeSub = comcode.substring(0,4);
          if(comcodeSub.equals("1101")) {
              return "北京";

          }else if(comcodeSub.equals("1201")){
              return "天津";
          }else if(comcodeSub.equals("1301")){
              return "河北";
          }else if(comcodeSub.equals("1401")){
              return "山西";
          }else if(comcodeSub.equals("1501")){
              return "内蒙古";
          }else if(comcodeSub.equals("2101")){
              return "辽宁";
          }else if(comcodeSub.equals("2102")){
              return "大连";
          }else if(comcodeSub.equals("2201")){
              return "吉林";
          }else if(comcodeSub.equals("2301")){
              return "黑龙江";
          }else if(comcodeSub.equals("3101")){
              return "上海";
          }else if(comcodeSub.equals("3106")){
              return "上海";
          }else if(comcodeSub.equals("3109")){
              return "总公司营业部";
          }else if(comcodeSub.equals("3111")){
              return "上海";
          }else if(comcodeSub.equals("3201")){
              return "江苏";
          }else if(comcodeSub.equals("3301")){
              return "浙江";
          }else if(comcodeSub.equals("3302")){
              return "宁波";
          }else if(comcodeSub.equals("3401")){
              return "安徽";
          }else if(comcodeSub.equals("3501")){
              return "福建";
          }else if(comcodeSub.equals("3502")){
              return "厦门";
          }else if(comcodeSub.equals("3601")){
              return "江西";
          }else if(comcodeSub.equals("3701")){
              return "山东";
          }else if(comcodeSub.equals("3702")){
              return "青岛";
          }else if(comcodeSub.equals("4101")){
              return "河南";
          }else if(comcodeSub.equals("4201")){
              return "湖北";
          }else if(comcodeSub.equals("4301")){
              return "湖南";
          }else if(comcodeSub.equals("4401")){
              return "广东";
          }else if(comcodeSub.equals("4403")){
              return "深圳";
          }else if(comcodeSub.equals("4501")){
              return "广西";
          }else if(comcodeSub.equals("4601")){
              return "海南";
          }else if(comcodeSub.equals("5001")){
              return "重庆";
          }else if(comcodeSub.equals("5101")){
              return "四川";
          }else if(comcodeSub.equals("5201")){
              return "贵州";
          }else if(comcodeSub.equals("5301")){
              return "云南";
          }else if(comcodeSub.equals("6101")){
              return "陕西";
          }else if(comcodeSub.equals("6201")){
              return "甘肃";
          }else if(comcodeSub.equals("6301")){
              return "青海";
          }else if(comcodeSub.equals("6401")){
              return "宁夏";
          }else if(comcodeSub.equals("6501")){
              return "新疆";
          }else {
              return "信息缺失";
          }
        }
    }

    public String business_code_channel(String bussinessnature2){
        String[] hezuods = {"503","507","1101","1102", "1104", "1105", "1106"};
        String[] biaozhundx = {"1103","1201","1202","1203","1204","1205","1206","1207","1208","1209","1210","1211","1212", "1213","1214","502","506","501","505","504","509","002","003","1000"};
        String[] netxiaoqd = {"1301","1302","1303","1304","1305","1401","1306","1307","1308","1309","1310","1402","1403","1300"};
        String[] cheshangqd = {"314","313","2201","2202","2204","302","2000","2200","2205","2206"};
        String[] shouxiandl ={"2304","304"};
        String[] zhixiaoqd = {"0201","0101","001","005","000","0100"};
        String[] gedaiqd = {"2401","100"};
        String[] zhuanyeqd = {"2101","200"};
        String[] jianyeqd = {"2105","305","306","312","310","999","2302","2203", "2103","2104"};
        String[] chongkeqd = {"004","0300","0301","2102","2500","400"};



        if(bussinessnature2.equals("")){
            return "unknown";

        }else if(useArrBinSea(hezuods, bussinessnature2)){
            return "合作电商";
        }else if(useArrBinSea(biaozhundx, bussinessnature2)){
            return "标准电销";
        }else if(useArrBinSea(netxiaoqd, bussinessnature2)){
            return "网销渠道";
        }else if(useArrBinSea(cheshangqd, bussinessnature2)){
            return "车商渠道";
        }else if(useArrBinSea(shouxiandl, bussinessnature2)){
            return "寿险代理";
        }else if(useArrBinSea(zhixiaoqd, bussinessnature2)){
            return "直销渠道";
        }else if(useArrBinSea(gedaiqd, bussinessnature2)){
            return "个代渠道";
        }else if(useArrBinSea(zhuanyeqd, bussinessnature2)){
            return "专业渠道";
        }else if(useArrBinSea(jianyeqd, bussinessnature2)){
            return "兼业渠道";
        }else if(useArrBinSea(chongkeqd, bussinessnature2)){
            return "重客渠道";
        }else {
            return "其他传统渠道";
        }

    }


    public static boolean useArrBinSea(String[] arr, String targetValue) {
        int a = findPositionFromArray(arr, targetValue);
        if (a >= 0)
            return true;
        else
            return false;
    }

    public static int findPositionFromArray(String[] arr, String targetValue){
            return ArrayUtils.indexOf(arr, targetValue);
    }

    //
    //public static void  main(String[] args){
    //    BjfxUserDF bjfxUserDF = new BjfxUserDF();
    //    //double db = 1.547;
    //    //System.out.println(db);
    //    //
    //    //String str = bjfxUserDF.evaluate("renewyear", "PDDB201736010730001124", "PDDB201736010730036685,PDDB201736010730001124,PDDB201636010730000220,PDDB201436010730028385,PDDB201436010730000206,PDDB201036010730011199", "0,1,1,1,1,0");
    //    //String str = bjfxUserDF.evaluate("channel","004");
    //    String str = bjfxUserDF.evaluate("channel","1101");
    //    //String[] str = "PDDB201736010730036685,PDDB201736010730001124,PDDB201636010730000220,PDDB201436010730028385,PDDB201436010730000206,PDDB201036010730011199".split(",");
    //    //int s = ArrayUtils.indexOf(str,"PDDB20173601073000112s4");
    //    System.out.println(str);
    //
    //}

}
