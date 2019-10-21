package com.itwang.Demo;

/**
 * @ProjectName: SchoolBigDataAnalysis
 * @Package: com.itwang.Demo
 * @ClassName: T1
 * @Author: hadoop
 * @Description:
 * @Date: 2019-04-03 10:58
 * @Version: 1.0
 */
public class T1 {
    public static void main(String[] args) {
        String p = "201508040134";
        String p1 = "201608040134";
        String p2= "201708040134";
        String p3 = "2010508040134";
        if (p.matches("2015.{8}")){
            System.out.println(true);
        }else{
            System.out.println(false);
        }



    }
}
