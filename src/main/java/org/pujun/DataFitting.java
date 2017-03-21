package org.pujun;

import com.mongodb.*;
import com.opencsv.CSVWriter;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Created by milletpu on 2017/3/16.
 */
public class DataFitting {
    ArrayList<String[]> result = new ArrayList<String[]>(); //结果result
    int time = 0;    //递归次数计数


    //链接数据库
    MongoClient client = new MongoClient("127.0.0.1", 27017);
    DB db = client.getDB("airdb");
    DBCollection pmDataDay = db.getCollection("pmdata_day");


    /**
     * 查询某个城市的PM25数据
     * @param cityName 城市名称
     * @param startTime 起始时间
     * @param endTime 结束时间
     * @return 查询到的数据数组
     * @throws ParseException
     */
    public double[] getDataByCity(String cityName, String startTime, String endTime) throws ParseException {
        //日期格式
        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        df.setCalendar(new GregorianCalendar(new SimpleTimeZone(0, "GMT")));
        Calendar cal = Calendar.getInstance();
        Date thisDate = df.parse(startTime);
        Date endDate = df.parse(endTime);

        //查询时间段内的该城市的PM25数据
        ArrayList<Double> dataSeries = new ArrayList<Double>();
        cal.setTime(df.parse(startTime));
        BasicDBObject query = new BasicDBObject();
        double thisDatePM25 = 0;
        while(thisDate.before(endDate)) {
            query.put("time", thisDate);
            query.put("city", cityName);
            DBCursor cur = pmDataDay.find(query);
            if (cur.hasNext()) {    //若存在此时刻的历史数据，则加入list中
                thisDatePM25 = Double.parseDouble(cur.next().get("pm25").toString());
                dataSeries.add(thisDatePM25);
            }
            cal.add(Calendar.DATE, 1);  //时间+1day循环
            thisDate = cal.getTime();
        }

        //ArrayList转数组
        double[] dataArray = new double[dataSeries.size()];
        for (int i = 0; i < dataSeries.size(); i++) {
            dataArray[i] = dataSeries.get(i);
        }

        //返回查询到的数据
        if (dataArray.length == 0){
            System.out.println("getDataByCity()：起始时间点没有数据，请更换起始时间startTime！");
            System.out.println("getDataByCity()：一共采集到 " + dataArray.length + " 个数据！");
            return null;
        }else {
            System.out.println("getDataByCity()：一共采集到 " + dataArray.length + " 个数据！");
            return dataArray;
        }


    }


    /**
     * 查询某个站点的数据
     * @param code 站点编号
     * @param startTime 起始时间
     * @param endTime 结束时间
     * @return 查询到的数据数组
     * @throws ParseException
     */
    public double[] getDataByStation(String code, String startTime, String endTime) throws ParseException {
        //日期格式
        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        df.setCalendar(new GregorianCalendar(new SimpleTimeZone(0, "GMT")));
        Calendar cal = Calendar.getInstance();
        Date thisDate = df.parse(startTime);
        Date endDate = df.parse(endTime);

        //查询时间段内的该城市的PM25数据
        ArrayList<Double> dataSeries = new ArrayList<Double>();
        cal.setTime(df.parse(startTime));
        BasicDBObject query = new BasicDBObject();
        double thisDatePM25 = 0;
        while(thisDate.before(endDate)) {
            query.put("time", thisDate);
            query.put("code", code);
            DBCursor cur = pmDataDay.find(query);
            if (cur.hasNext()) {    //若存在此时刻的历史数据，则加入list中
                thisDatePM25 = Double.parseDouble(cur.next().get("pm25").toString());
                dataSeries.add(thisDatePM25);
            }
            cal.add(Calendar.DATE, 1);  //时间+1day循环
            thisDate = cal.getTime();
        }

        //ArrayList转数组
        double[] dataArray = new double[dataSeries.size()];
        for (int i = 0; i < dataSeries.size(); i++) {
            dataArray[i] = dataSeries.get(i);
        }

        //返回查询到的数据数组
        if (dataArray.length == 0){
            System.out.println("getDataByStation()：起始时间点没有数据，请更换起始时间startTime！");
            System.out.println("getDataByStation()：一共采集到 " + dataArray.length + " 个数据！");
            return null;
        }else {
            System.out.println("getDataByStation()：一共采集到 " + dataArray.length + " 个数据！");
            return dataArray;
        }

    }



    //递归出口
    public void out(int l,int r,double[] ans, int maxNum) throws IOException {
        //递归出口打印结果
        String[] res = new String[r];
        for(int i = l;i<=r; i++){
            //System.out.print(ans[i] + ", ");
            res[i-1] = String.valueOf(ans[i]);
        }
        //System.out.println("\n");
        if(result.size()<maxNum) {
            result.add(res);
        }


    }


    /**
     * 找所有上升
     * @param step 时间戳（初始化为1）
     * @param ans_have_len 表示ans已经存放的数据长度（递归使用，初始化为0）
     * @param num_len num数组的总长度
     * @param ans_len ans最终留下的长度
     * @param ans 结果数组
     * @param num 原始数据
     */
    public void up(int step,int ans_have_len,int num_len,int ans_len,double[] ans,double[] num, int maxNum, int maxTime) throws IOException {
        time++;
        if (time < maxTime) {
            num[0] = ans[0] = Double.NEGATIVE_INFINITY;
            if (ans_have_len == ans_len) {
                out(1, ans_len, ans, maxNum);
                return;
            }
            for (int i = step; i < num_len; i++) {
                if (num[i] > ans[ans_have_len]) {
                    ans[ans_have_len + 1] = num[i];
                    up(i + 1, ans_have_len + 1, num_len, ans_len, ans, num, maxNum, maxTime);
                }
            }
        }

    }

    /**
     * 找所有下降
     * @param step 时间戳（初始化为1）
     * @param ans_have_len 表示ans已经存放的数据长度（递归使用，初始化为0）
     * @param num_len num数组的总长度
     * @param ans_len ans最终留下的长度
     * @param ans 结果数组
     * @param num 原始数据
     */
    public void down(int step,int ans_have_len,int num_len,int ans_len,double[] ans,double[] num, int maxNum, int maxTime) throws IOException {
        time++;
        if (time < maxTime) {
            num[0] = ans[0] = Double.POSITIVE_INFINITY;
            if (ans_have_len == ans_len) {
                out(1, ans_len, ans, maxNum);
                return;
            }
            for (int i = step; i < num_len; i++) {
                if (num[i] < ans[ans_have_len]) {
                    ans[ans_have_len + 1] = num[i];
                    down(i + 1, ans_have_len + 1, num_len, ans_len, ans, num, maxNum, maxTime);
                }
            }
        }
    }



    /**
     * 找所有峰值
     * @param step 时间戳（初始化为1）
     * @param ans_have_len 表示ans已经存放的数据长度（递归使用，初始化为0）
     * @param k 峰值顶点处
     * @param num_len num数组的总长度
     * @param ans_len ans最终留下的长度
     * @param ans 结果数组
     * @param num 原始数据
     */
    public void peak(int step,int ans_have_len,int k,int num_len,int ans_len,double[] ans,double[] num, int maxNum, int maxTime) throws IOException {
        time++;
        if (time < maxTime) {
            num[0] = ans[0] = Double.NEGATIVE_INFINITY;
            if (ans_have_len <= k) {
                if (ans_have_len == ans_len) {
                    out(1, ans_len, ans, maxNum);
                    return;
                }
                for (int i = step; i < num_len; i++) {
                    if (num[i] > ans[ans_have_len]) {
                        ans[ans_have_len + 1] = num[i];
                        peak(i + 1, ans_have_len + 1, k, num_len, ans_len, ans, num, maxNum, maxTime);
                    }
                }
            } else {
                if (ans_have_len == ans_len) {
                    out(1, ans_len, ans, maxNum);
                    return;
                }
                for (int i = step; i < num_len; i++) {
                    if (num[i] < ans[ans_have_len]) {
                        ans[ans_have_len + 1] = num[i];
                        peak(i + 1, ans_have_len + 1, k, num_len, ans_len, ans, num, maxNum, maxTime);
                    }
                }
            }
        }
    }

    /**
     * 找所有低谷
     * @param step 时间戳（初始化为1）
     * @param ans_have_len 表示ans已经存放的数据长度（递归使用，初始化为0）
     * @param k 低估顶点处
     * @param num_len num数组的总长度
     * @param ans_len ans最终留下的长度
     * @param ans 结果数组
     * @param num 原始数据
     */
    public void trough(int step,int ans_have_len,int k,int num_len,int ans_len,double[] ans,double[] num, int maxNum, int maxTime) throws IOException {
        time++;
        if (time < maxTime) {
            num[0] = ans[0] = Double.POSITIVE_INFINITY;
            if (ans_have_len <= k) {
                if (ans_have_len == ans_len) {
                    out(1, ans_len, ans, maxNum);
                    return;
                }
                for (int i = step; i < num_len; i++) {
                    if (num[i] < ans[ans_have_len]) {
                        ans[ans_have_len + 1] = num[i];
                        trough(i + 1, ans_have_len + 1, k, num_len, ans_len, ans, num, maxNum, maxTime);
                    }
                }
            } else {
                if (ans_have_len == ans_len) {
                    out(1, ans_len, ans, maxNum);
                    return;
                }
                for (int i = step; i < num_len; i++) {
                    if (num[i] > ans[ans_have_len]) {
                        ans[ans_have_len + 1] = num[i];
                        trough(i + 1, ans_have_len + 1, k, num_len, ans_len, ans, num, maxNum, maxTime);
                    }
                }
            }
        }
    }


    /**
     * 从数组中挖掘出符合特征的子数组
     * @param data 传入的数组
     * @param type 子数组类型 - 上升、下降、峰值、低谷
     * @param len 子数组长度 - 4、8、12、16、20、24、32、40
     */
    public void getDataWithTrend(double[] data, String type, int len, int maxNum, int maxTime) throws IOException {
        double[] ans = new double[100];

        if (type.equals("up")) {
            up(1, 0, data.length, len, ans, data, maxNum, maxTime);
            String fileAdd = len + "up" +".csv";
            File file = new File(fileAdd);
            Writer writer = new FileWriter(file);
            CSVWriter cw = new CSVWriter(writer, ',');
            for (String[] aResult : result) cw.writeNext(aResult); //写入csv
            cw.close();
            if (result.size()==0){
                System.out.println("Failed:  getDataWithTrend()."+type+"     没有找到长度为" +len+ "的子数组！");
            }else{
                System.out.println("Succeed: getDataWithTrend()."+type+"     找到了长度为" +len+ "的子数组" + result.size()+ "个。");
            }
            result.clear();
            time = 0;


        } else if (type.equals("down")) {
            down(1, 0, data.length, len, ans ,data, maxNum, maxTime);
            String fileAdd = len + "down" +".csv";
            File file = new File(fileAdd);
            Writer writer = new FileWriter(file);
            CSVWriter cw = new CSVWriter(writer, ',');
            for (String[] aResult : result) cw.writeNext(aResult); //写入csv
            cw.close();
            if (result.size()==0){
                System.out.println("Failed:  getDataWithTrend()."+type+"   没有找到长度为" +len+ "的子数组！");
            }else{
                System.out.println("Succeed: getDataWithTrend()."+type+"   找到了长度为" +len+ "的子数组" + result.size()+ "个。");
            }
            result.clear();
            time = 0;

        } else if (type.equals("peak")) {
            peak(1, 0, 4, data.length, len, ans, data, maxNum, maxTime);
            String fileAdd = len + "peak" +".csv";
            File file = new File(fileAdd);
            Writer writer = new FileWriter(file);
            CSVWriter cw = new CSVWriter(writer, ',');
            for (String[] aResult : result) cw.writeNext(aResult); //写入csv
            cw.close();
            if (result.size()==0){
                System.out.println("Failed:  getDataWithTrend()."+type+"   没有找到长度为" +len+ "的子数组！");
            }else{
                System.out.println("Succeed: getDataWithTrend()."+type+"   找到了长度为" +len+ "的子数组" + result.size()+ "个。");
            }
            result.clear();
            time = 0;

        } else if (type.equals("trough")) {
            trough(1, 0, 4, data.length, len, ans, data, maxNum, maxTime);
            String fileAdd = len + "trough" +".csv";
            File file = new File(fileAdd);
            Writer writer = new FileWriter(file);
            CSVWriter cw = new CSVWriter(writer, ',');
            for (String[] aResult : result) cw.writeNext(aResult); //写入csv
            cw.close();
            if (result.size()==0){
                System.out.println("Failed:  getDataWithTrend()."+type+" 没有找到长度为" +len+ "的子数组！");
            }else{
                System.out.println("Succeed: getDataWithTrend()."+type+" 找到了长度为" +len+ "的子数组" + result.size()+ "个。");
            }
            result.clear();
            time = 0;

        }

        //无返回值

    }


    public static void main(String[] args) throws ParseException, IOException {
        //double[] testData = {20,12,15,2,10,50,32,129,29,30,27,86,15,44,287,235,12,9,3,42,3,2,443,6};

        DataFitting df = new DataFitting();
        double[] data = df.getDataByStation("1299A", "2014-11-01 00:00:00", "2015-11-01 00:00:00");
        df.getDataWithTrend(data, "peak", 32, 20, 10000);

//        for(int len =4; len<=32; len=len+4) {
//            df.getDataWithTrend(data, "up", len, 20, 10000);
//            df.getDataWithTrend(data, "down", len, 20, 10000);
//            df.getDataWithTrend(data, "peak", len, 20, 10000);
//            df.getDataWithTrend(data, "trough", len, 20, 10000);
//        }

    }







}
