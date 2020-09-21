package net.ccic.sparkprocess.hive.findky;

import org.apache.hadoop.hive.ql.exec.UDF;

/**
 * Created by FrankSen on 2019/2/28.
 */
public class VechicleType extends UDF implements VechicleTypeInterface {
    /**
     * Create time: 2019-02-28
     * Author: Wys lij jjl
     * Function name: f_get_cartype()
     * @param intg_datasource
     * @param username
     * @param bussinesssort
     * @param carkindcode
     * @param usernaturecode
     * @param standard_name
     * @param cartype
     * @param std_part2
     * @param std_part3
     * @return
     */



     UDFUtils udfUtils = new UDFUtils();

    public static void main(String[] args){

        VechicleType carType = new VechicleType();

        String res_value = carType.evaluate("03", "J特种车", "104"
                                               , "TZ" ,"84", "中油ZYT5253TSS输砂车"
                                                ,"","5253","TSS");
        System.out.println(res_value);
    }

    public String evaluate(String intg_datasource,String username, String bussinesssort
                           ,String carkindcode, String usernaturecode, String standard_name
                           ,String cartype, String std_part2, String std_part3){

        // 98
        if(intg_datasource.equals("98")){

            return vehicleTypeLevel98(username, bussinesssort, carkindcode
                               ,usernaturecode,standard_name,cartype
                               ,std_part2,std_part3);


        // !=98
        }else {

           return vehicleTypeLevelNot98(username, bussinesssort, carkindcode
                                  ,usernaturecode,standard_name,cartype
                                  ,std_part2,std_part3);


        }

    }

    @Override
    public String vehicleTypeLevel98(String usename, String bussinesssort
            ,String carkindcode, String usernaturecode, String standard_name
            ,String cartype, String std_part2, String std_part3) {
        //usename判断
        String res = null;

        String[] business01 = {"130", "120", "200", "500", "900", "240","210"};
        String[] usename3 = {"I营业挂车", "H非营业挂车"};
        String[] carkindcode01 = {"11", "21"};





        if(usename.equals("B非营业客车")){
            if(udfUtils.useArrBinSed(business01,bussinesssort)){
                res = "党政机关、事业团体非营业客车";
            }else{
                res = "企业非营业客车";
            }

        }else if(usename.equals("E营业客车")){
            if(udfUtils.useArrBinSed(carkindcode01,carkindcode)){
                if(usernaturecode.equals("101")){
                    res = "出租、租赁营业客车";
                }else if(usernaturecode.equals("95")){
                    res = "预约出租租赁";

                }else if(usernaturecode.equals("102")){
                    res = "城市公交营业客车";

                }else if(usernaturecode.equals("103")){
                    res = "公路客运营业客车";
                }else{
                    res = "其他营业客车";
                }
            }else{
                res = "其他营业客车";
            }

            //军乐
        }else if(udfUtils.useArrBinSed(usename3, usename)){
            if (udfUtils.like(standard_name, "集装箱")) {
                res = "A集装箱挂车";
            } else {
                res = "B非集装箱挂车";
            }


        }else {
            res = getOtherCarType(usename, standard_name, cartype
                     , usename3, std_part2, std_part3);
        }


        return res;
    }



    @Override
    public String vehicleTypeLevelNot98(String usename, String bussinesssort, String carkindcode, String usernaturecode, String standard_name, String cartype, String std_part2, String std_part3) {
        //businesssort判断
        //usenaturecode、carkindcode判断
        //standard_name判断
        //cartype判断
        //usename判断
        String res = null;

        String[] business01 = {"15", "16", "48", "61", "62", "65", "81", "82", "83"};
        String[] usename3 = {"I营业挂车", "H非营业挂车"};

        String[] usenaturecode01 = {"82", "86", "90"};
        String[] usenaturecode02 = { "87", "88"};

        String[] carkindcode01 = {"A0", "B0", "L0"};


        if(usename.equals("B非营业客车")){
            if(udfUtils.useArrBinSed(business01, bussinesssort)){
                res = "党政机关、事业团体非营业客车";
            }else{
                res = "企业非营业客车";
            }

        }else if(usename.equals("E营业客车")){
            if(udfUtils.useArrBinSed(carkindcode01,carkindcode)){
                if(udfUtils.useArrBinSed(usenaturecode01, usernaturecode)){
                    res = "出租、租赁营业客车";
                }else if(usernaturecode.equals("95")){
                    res = "预约出租租赁";

                }else if(usernaturecode.equals("89")){
                    res = "城市公交营业客车";

                }else if(udfUtils.useArrBinSed(usenaturecode02, usernaturecode)){
                    res = "公路客运营业客车";
                }
            }else{
                res = "其他营业客车";
            }

            //军乐
        }else {
            res = getOtherCarType(usename, standard_name, cartype, usename3, std_part2, std_part3);
        }


        return res;
    }

    private String getOtherCarType(String usename, String standard_name, String cartype
                     , String[] usename3, String std_part2, String std_part3) {
        String res = null;

        if(udfUtils.useArrBinSed(usename3, usename)){
            res = getCarTypeBox(standard_name);


        }else if(usename.equals("D营业货车")){


            res = getCarTypeUseNature(cartype, std_part2, std_part3,"其他营业货车");


            //浩杰
        }else if(usename.equals("C非营业货车")){

            res = getCarTypeUseNature(cartype, std_part2, std_part3,"其他非营业货车");

        }else if(usename.equals("J特种车")){

            if(std_part2.equals("")){

                res = getJSecpialCarType(std_part3);
            }else{

                String std_part2_0_1 = std_part2.substring(0, 1);

                if(std_part2_0_1.equals("4")){
                    res = "牵引车";

                } else if (std_part2_0_1.equals("9")) {
                    res = "挂车";

                }else{
                    res = getJSecpialCarType(std_part3);
                }
            }


        }
        return res;
    }

    private String getCarTypeBox(String standard_name) {
        String res;
        if (udfUtils.like(standard_name, "集装箱")) {
            res = "A集装箱挂车";
        } else {
            res = "B非集装箱挂车";
        }
        return res;
    }


    private String getCarTypeUseNature(String cartype, String std_part2, String std_part3, String change_val) {
        String res = null;
        // std_part2/std_part3 is null
        String std_part2_0_1 = null;
        String std_part3_0_1 = null;



        if(cartype.equals("1")){
            res = "C牵引车";

        }else if(cartype.equals("2")){
            res = "D自卸车";
        }else {

            //为赋值准备
            if(std_part2.length() <= 0 ){

                std_part2_0_1 = "";
            }else {
                std_part2_0_1 = std_part2.substring(0, 1);
            }

            if(std_part3.length() <= 0){
                std_part3_0_1 = "";
            }else{
                std_part3_0_1 = std_part3.substring(0, 1);
            }

            if (std_part2_0_1.equals("4")) {
                res = "C牵引车";
            } else if ( std_part2_0_1.equals("3")) {
                res = "D自卸车";
            } else if (std_part2_0_1.equals("5") && std_part3_0_1.equals("C")) {
                res = "E仓栅车";

            } else if (std_part2_0_1.equals("5") && std_part3_0_1.equals("X")) {
                res = "F厢式车";

            }else{
                res = change_val;
            }
        }


        return res;
    }

    /**
     * J特种车 除牵引车和挂车之外的车辆类型判断
     * @param STD_part3
     * @return
     */
    private String getJSecpialCarType(String STD_part3) {
        String res =null;
        //UDFUtils udfUtils=new UDFUtils();
        String[] str_J= {"GK","HQ","QX","QZ","SP","SQ","XF","YK"};
        String[] str_X= {"BW","BY","CC","CS","DS","FY","GC","GY","JB","JC","JE","JH","JL","JQ","JX","LC","KC","LY","QC","QY","SC","SH","SS","SY","TS","TX","XB","XC","XD","XF","XJ","XL","XY","YC","YK","YQ","YZ","ZD","ZH","ZS","ZX"};
        String[] str_G= {"CL","DF","DY","FL","FS","GS","HY","JB","JJ","JY","LQ","LY","PS","QX","SN","SS","XE","XF","XH","XW","YQ","YS","YY"};
        String[] str_T= {"AZ","CJ","CL","CS","CT","CY","DF","DM","DY","DZ","FS","GH","GL","GY","HB","HS","HZ","JC","JK","JZ","KT","LF","LG","PY","QL","QX","QZ","SJ","SL","SM","SN","SS","TJ","TL","TP","XF","XJ","XL","YB","YC","YG","YH","YL","YS","YA","ZJ","ZM","ZY"};


        //判断书否为大类车
        if(STD_part3.equals("")){
            res = "其他特种车";
        }else {

            String STD_part3_1 = null;
            String STD_part3_3 = null;
            String STD_part3_1_3 = null;
            if(STD_part3.length() <= 2){
                STD_part3_1 = STD_part3.substring(0,1);
                STD_part3_1_3 = "";
                STD_part3_3 ="";
            }else{
                STD_part3_1 = STD_part3.substring(0,1);
                STD_part3_1_3 = STD_part3.substring(1, 3);
                STD_part3_3=STD_part3.substring(0,3);
            }

            if(STD_part3_1.equals("J")) {
                if(!udfUtils.useArrBinSed(str_J,STD_part3_1_3)){
                    res="起重举升汽车";
                }else  if(STD_part3_3.equals("JGK")) {
                    res ="起重举升-高空作业车" ;
                }
                else if(STD_part3_3.equals("JHQ")) {
                    res="起重举升-后栏板起重运输车" ;
                }
                else if(STD_part3_3.equals("JQX")) {
                    res="起重举升-飞机清洗车" ;
                }
                else if(STD_part3_3.equals("JQZ")) {
                    res="起重举升-汽车起重机" ;
                }
                else if(STD_part3_3.equals("JSP")) {
                    res="起重举升-航空食品装运车" ;
                }
                else if(STD_part3_3.equals("JSQ")) {
                    res="起重举升-随车起重运输车" ;
                }
                else if(STD_part3_3.equals("JXF")) {
                    res="起重举升-消防车" ;
                }
                else if(STD_part3_3.equals("JYK")) {
                    res="起重举升-翼开启栏板起重运输车" ;
                }else{
                    res ="起重举升汽车";
                }
            }
            else if(STD_part3_1.equals("X")) {
                if(!udfUtils.useArrBinSed(str_X, STD_part3_1_3)){
                    res="厢式车";
                }else if(STD_part3_3.equals("XBW")) {
                    res="厢式车-保温车" ;
                }
                else if(STD_part3_3.equals("XBY")) {
                    res="厢式车-殡仪车" ;
                }
                else if(STD_part3_3.equals("XCC")) {
                    res="厢式车-餐车" ;
                }
                else if(STD_part3_3.equals("XCS")) {
                    res="厢式车-厕所车" ;
                }
                else if(STD_part3_3.equals("XDS")) {
                    res="厢式车-电视车" ;
                }
                else if(STD_part3_3.equals("XFY")) {
                    res="厢式车-防疫车" ;
                }
                else if(STD_part3_3.equals("XGC")) {
                    res="厢式车-工程车" ;
                }
                else if(STD_part3_3.equals("XGY")) {
                    res="厢式车-化验车" ;
                }
                else if(STD_part3_3.equals("XJB")) {
                    res="厢式车-警备车" ;
                }
                else if(STD_part3_3.equals("XJC")) {
                    res="厢式车-检测车" ;
                }
                else if(STD_part3_3.equals("XJE")) {
                    res="厢式车-监测车" ;
                }
                else if(STD_part3_3.equals("XJH")) {
                    res="厢式车-救护车" ;
                }
                else if(STD_part3_3.equals("XJL")) {
                    res="厢式车-计量车" ;
                }
                else if(STD_part3_3.equals("XJQ")) {
                    res="厢式车-警犬运输车" ;
                }
                else if(STD_part3_3.equals("XJX")) {
                    res="厢式车-检修车" ;
                }
                else if(STD_part3_3.equals("XLC")) {
                    res="厢式车-冷藏车" ;
                }
                else if(STD_part3_3.equals("XKC")) {
                    res="厢式车-勘察车" ;
                }
                else if(STD_part3_3.equals("XLY")) {
                    res="厢式车-淋浴车" ;
                }
                else if(STD_part3_3.equals("XQC")) {
                    res="厢式车-囚车" ;
                }
                else if(STD_part3_3.equals("XQY")) {
                    res="厢式车-爆破器材运输车" ;
                }
                else if(STD_part3_3.equals("XSC")) {
                    res="厢式车-伤残运送车" ;
                }
                else if(STD_part3_3.equals("XSH")) {
                    res="厢式车-售货车" ;
                }
                else if(STD_part3_3.equals("XSS")) {
                    res="厢式车-手术车" ;
                }
                else if(STD_part3_3.equals("XSY")) {
                    res="厢式车-计划生育车" ;
                }
                else if(STD_part3_3.equals("XTS")) {
                    res="厢式车-图书馆车" ;
                }
                else if(STD_part3_3.equals("XTX")) {
                    res="厢式车-通讯车" ;
                }
                else if(STD_part3_3.equals("XXB")) {
                    res="厢式车-厢容可变车" ;
                }
                else if(STD_part3_3.equals("XXC")) {
                    res="厢式车-宣传车" ;
                }
                else if(STD_part3_3.equals("XXD")) {
                    res="厢式车-消毒车" ;
                }
                else if(STD_part3_3.equals("XXF")) {
                    res="厢式车-通讯指挥/消防车" ;
                }
                else if(STD_part3_3.equals("XXJ")) {
                    res="厢式车-血浆运输车" ;
                }
                else if(STD_part3_3.equals("XXL")) {
                    res="厢式车-修理车" ;
                }
                else if(STD_part3_3.equals("XXY")) {
                    res="厢式车-厢式运输车" ;
                }
                else if(STD_part3_3.equals("XYC")) {
                    res="厢式车-运钞车" ;
                }
                else if(STD_part3_3.equals("XYK")) {
                    res="厢式车-翼开启厢式车" ;
                }
                else if(STD_part3_3.equals("XYQ")) {
                    res="厢式车-仪器车" ;
                }
                else if(STD_part3_3.equals("XYZ")) {
                    res="厢式车-邮政车" ;
                }
                else if(STD_part3_3.equals("XZD")) {
                    res="厢式车-X射线诊断车" ;
                }
                else if(STD_part3_3.equals("XZH")) {
                    res="厢式车-指挥车" ;
                }
                else if(STD_part3_3.equals("XZS")) {
                    res="厢式车-住宿车" ;
                }
                else if(STD_part3_3.equals("XZX")) {
                    res="厢式车-地震装线车" ;
                }else{
                    res = "厢式车";
                }
            }
            else if(STD_part3_1.equals("G")) {
                if(!udfUtils.useArrBinSed(str_G,STD_part3_1_3)){
                    res="罐车";
                }else if(STD_part3_3.equals("GCL")) {
                    res="罐车-油井液处理车" ;
                }
                else if(STD_part3_3.equals("GDF")) {
                    res="罐车-散装电石粉车" ;
                }
                else if(STD_part3_3.equals("GDY")) {
                    res="罐车-低温液体运输车" ;
                }
                else if(STD_part3_3.equals("GFL")) {
                    res="罐车-粉粒物料运输车" ;
                }
                else if(STD_part3_3.equals("GFS")) {
                    res="罐车-粒粒食品运输车" ;
                }
                else if(STD_part3_3.equals("GGS")) {
                    res="罐车-供水车" ;
                }
                else if(STD_part3_3.equals("GHY")) {
                    res="罐车-化工液体运输车" ;
                }
                else if(STD_part3_3.equals("GJB")) {
                    res="罐车-混凝土搅拌运输车" ;
                }
                else if(STD_part3_3.equals("GJJ")) {
                    res="罐车-飞机加油车" ;
                }
                else if(STD_part3_3.equals("GJY")) {
                    res="罐车-加油车" ;
                }
                else if(STD_part3_3.equals("GLQ")) {
                    res="罐车-沥青洒布车" ;
                }
                else if(STD_part3_3.equals("GLY")) {
                    res="罐车-沥青运输车" ;
                }
                else if(STD_part3_3.equals("GPS")) {
                    res="罐车-绿化喷洒车" ;
                }
                else if(STD_part3_3.equals("GQX")) {
                    res="罐车-清洗车" ;
                }
                else if(STD_part3_3.equals("GSN")) {
                    res="罐车-散装水泥" ;
                }
                else if(STD_part3_3.equals("GSS")) {
                    res="罐车-水车" ;
                }
                else if(STD_part3_3.equals("GXE")) {
                    res="罐车-吸粪车" ;
                }
                else if(STD_part3_3.equals("GXF")) {
                    res="罐车-水罐消防车/泡沫消防车/供水消防车" ;
                }
                else if(STD_part3_3.equals("GXH")) {
                    res="罐车-下灰车" ;
                }
                else if(STD_part3_3.equals("GXW")) {
                    res="罐车-吸污车" ;
                }
                else if(STD_part3_3.equals("GYQ")) {
                    res="罐车-液化气体运输车" ;
                }
                else if(STD_part3_3.equals("GYS")) {
                    res="罐车-液态食品运输车" ;
                }
                else if(STD_part3_3.equals("GYY")) {
                    res="罐车-运油车" ;
                }else{
                    res = "罐车";
                }
            }
            else if(STD_part3_1.equals("T")) {
                if(!udfUtils.useArrBinSed(str_T,STD_part3_1_3)){
                    res="其他特种车";
                }
                else if(STD_part3_3.equals("TAZ")) {
                    res="特种车-井架安装车" ;
                }
                else if(STD_part3_3.equals("TCJ")) {
                    res="特种车-测井车" ;
                }
                else if(STD_part3_3.equals("TCL")) {
                    res="特种车-车辆运输车" ;
                }

                else if(STD_part3_3.equals("TCS")) {
                    res="特种车-测试井架车" ;
                }
                else if(STD_part3_3.equals("TCT")) {
                    res="特种车-静力触探车" ;
                }
                else if(STD_part3_3.equals("TCY")) {
                    res="特种车-采油车" ;
                }
                else if(STD_part3_3.equals("TDF")) {
                    res="特种车-氮气发生车" ;
                }
                else if(STD_part3_3.equals("TDM")) {
                    res="特种车-地锚车" ;
                }
                else if(STD_part3_3.equals("TDY")) {
                    res="特种车-电源车" ;
                }
                else if(STD_part3_3.equals("TDZ")) {
                    res="特种车-氮气增压车" ;
                }
                else if(STD_part3_3.equals("TFS")) {
                    res="特种车-油井防砂车" ;
                }
                else if(STD_part3_3.equals("TGH")) {
                    res="特种车-固井管汇车" ;
                }
                else if(STD_part3_3.equals("TGL")) {
                    res="特种车-锅炉车" ;
                }
                else if(STD_part3_3.equals("TGY")) {
                    res="特种车-供液泵车" ;
                }
                else if(STD_part3_3.equals("THB")) {
                    res="特种车-混凝土泵车" ;
                }
                else if(STD_part3_3.equals("THS")) {
                    res="特种车-混砂车" ;
                }
                else if(STD_part3_3.equals("THZ")) {
                    res="特种车-炸药混装车" ;
                }
                else if(STD_part3_3.equals("TJC")) {
                    res="特种车-洗井车" ;
                }
                else if(STD_part3_3.equals("TJK")) {
                    res="特种车-井控管汇车" ;
                }
                else if(STD_part3_3.equals("TJZ")) {
                    res="特种车-集装箱运输车" ;
                }
                else if(STD_part3_3.equals("TKT")) {
                    res="特种车-机场客梯车" ;
                }
                else if(STD_part3_3.equals("TLF")) {
                    res="特种车-立放井架车" ;
                }
                else if(STD_part3_3.equals("TLG")) {
                    res="特种车-连续管作业车" ;
                }
                else if(STD_part3_3.equals("TPY")) {
                    res="特种车-排液车" ;
                }
                else if(STD_part3_3.equals("TQL")) {
                    res="特种车-清蜡车" ;
                }
                else if(STD_part3_3.equals("TQX")) {
                    res="特种车-抢险车" ;
                }
                else if(STD_part3_3.equals("TQZ")) {
                    res="特种车-清障车" ;
                }
                else if(STD_part3_3.equals("TSJ")) {
                    res="特种车-试井车" ;
                }
                else if(STD_part3_3.equals("TSL")) {
                    res="特种车-扫路车" ;
                }
                else if(STD_part3_3.equals("TSM")) {
                    res="特种车-沙漠车" ;
                }
                else if(STD_part3_3.equals("TSN")) {
                    res="特种车-固井水泥车" ;
                }
                else if(STD_part3_3.equals("TSS")) {
                    res="特种车-输沙车" ;
                }
                else if(STD_part3_3.equals("TTJ")) {
                    res="特种车-通井车" ;
                }
                else if(STD_part3_3.equals("TTL")) {
                    res="特种车-投捞车" ;
                }
                else if(STD_part3_3.equals("TTP")) {
                    res="特种车-调剖车" ;
                }
                else if(STD_part3_3.equals("TXF")) {
                    res="特种车-消防车" ;
                }
                else if(STD_part3_3.equals("TXJ")) {
                    res="特种车-修井机" ;
                }
                else if(STD_part3_3.equals("TXL")) {
                    res="特种车-洗井清蜡车" ;
                }
                else if(STD_part3_3.equals("TYB")) {
                    res="特种车-抽油泵运输车" ;
                }
                else if(STD_part3_3.equals("TYC")) {
                    res="特种车-放射性源车" ;
                }
                else if(STD_part3_3.equals("TYG")) {
                    res="特种车-压裂管汇车" ;
                }
                else if(STD_part3_3.equals("TYH")) {
                    res="特种车-路面养护车" ;
                }
                else if(STD_part3_3.equals("TYL")) {
                    res="特种车-压裂车" ;
                }
                else if(STD_part3_3.equals("TYS")) {
                    res="特种车-压缩机车" ;
                }
                else if(STD_part3_3.equals("TYA")) {
                    res="特种车-运材车" ;
                }
                else if(STD_part3_3.equals("TZJ")) {
                    res="特种车-钻机车" ;
                }
                else if(STD_part3_3.equals("TZM")) {
                    res="特种车-照明车" ;
                }
                else if(STD_part3_3.equals("TZY")) {
                    res="特种车-可控震源车" ;
                }else{
                    res = "其他特种车";
                }
            }
            //全部条件都无法满足
            else {
                res="其他特种车";
            }
        }
        //返回车辆类型
        return res;
    }

}
