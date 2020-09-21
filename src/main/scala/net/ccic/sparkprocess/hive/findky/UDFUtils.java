package net.ccic.sparkprocess.hive.findky;

import org.apache.commons.lang3.ArrayUtils;

/**
 * Created by FrankSen on 2019/2/28.
 */
public class UDFUtils {

    /**
     *
     * @param riskcodeList
     * @param value
     * @return
     */
    public boolean useArrBinSed(String[] riskcodeList,String value) {
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

    public  int findPositionFromArray(String[] arr, String targetValue){
        return ArrayUtils.indexOf(arr, targetValue);
    }

    /**
     *
     * @param a
     * @param b
     * @return
     */
    public  boolean like(String a, String b) {
        if (a.matches(".*" + b + ".*")) {
            return true;
        }
        return false;
    }

}
