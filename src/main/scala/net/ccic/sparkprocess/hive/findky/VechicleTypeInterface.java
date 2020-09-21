package net.ccic.sparkprocess.hive.findky;

/**
 * Created by FrankSen on 2019/2/28.
 */
public interface VechicleTypeInterface {
    /**
     * 第一层 字段 intg_datasource 98 or ！98
     */
    String vehicleTypeLevel98(String usename, String bussinesssort
            , String carkindcode, String usernaturecode, String standard_name
            , String cartype, String std_part2, String std_part3);

    /**
     * 第二层 usename
     */
    String vehicleTypeLevelNot98(String usename, String bussinesssort
            , String carkindcode, String usernaturecode, String standard_name
            , String cartype, String std_part2, String std_part3);



}
