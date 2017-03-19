package org.pujun;

import com.mongodb.*;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Created by milletpu on 2017/3/16.
 */
public class DataFitting {

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


    /**
     * 从数组中挖掘出符合特征的子数组
     * @param data 传入的数组
     * @param type 子数组类型 - 上升、下降、峰值、低谷
     * @param len 子数组长度 - 4、8、12、16、20、24、32、40
     * @param maxNum 子数组个数
     */
    public double[] getDataWithTrend(double[] data, String type, int len, int maxNum){
        double[] resultOne = new double[len];
        ArrayList<Double> resultArray = new ArrayList<Double>();
        for(int i = 0; i< resultOne.length; i++) resultOne[i] = -1;

        if (type.equals("up")) {
            int n = 0;
            for (int i = 0; i < data.length; i++) {
                resultOne[n] = data[i];
                double lastResult = data[i];
                for (int j = i + 1; j < data.length; j++) {
                    if (data[j] > lastResult && n < len - 1) {
                        resultOne[++n] = data[j];
                        lastResult = data[j];
                    }
                }

//                if (n == len - 1) break;
//                else n = 0;
                if (n == len - 1) {
                    for (double aResult : resultOne) resultArray.add(aResult);
                    n = 0;
                } else {
                    n = 0;
                }
            }



        } else if (type.equals("down")) {
            int n = 0;
            for (int i = 0; i<data.length; i++) {
                resultOne[n] = data[i];
                double lastResult = data[i];
                for (int j = i + 1; j < data.length; j++) {
                    if (data[j] < lastResult && n < len-1) {
                        resultOne[++n] = data[j];
                        lastResult = data[j];
                    }
                }

//                if (n == len - 1) break;
//                else n = 0;
                if (n == len - 1) {
                    for (double aResult : resultOne) resultArray.add(aResult);
                    n = 0;
                } else {
                    n = 0;
                }
            }

        } else if (type.equals("peak")) {
            int n = 0;
            for (int i = 0; i<data.length; i++) {
                resultOne[n] = data[i];
                double lastResult = data[i];
                for (int j = i + 1; j < data.length; j++) {
                    if (data[j] > lastResult && n < (len/2)) {
                        resultOne[++n] = data[j];
                        lastResult = data[j];
                    }
                    if (data[j] < lastResult && n >= (len/2) && n < len-1) {
                        resultOne[++n] = data[j];
                        lastResult = data[j];
                    }
                }

//                if (n == len - 1) break;
//                else n = 0;
                if (n == len - 1) {
                    for (double aResult : resultOne) resultArray.add(aResult);
                    n = 0;
                } else {
                    n = 0;
                }
            }

        } else if (type.equals("trough")) {
            int n = 0;
            for (int i = 0; i<data.length; i++) {
                resultOne[n] = data[i];
                double lastResult = data[i];
                for (int j = i + 1; j < data.length; j++) {
                    if (data[j] < lastResult && n < (len/2)) {
                        resultOne[++n] = data[j];
                        lastResult = data[j];
                    }
                    if (data[j] > lastResult && n >= (len/2) && n < len-1) {
                        resultOne[++n] = data[j];
                        lastResult = data[j];
                    }
                }

//                if (n == len - 1) break;
//                else n = 0;
                if (n == len - 1) {
                    for (double aResult : resultOne) resultArray.add(aResult);
                    n = 0;
                } else {
                    n = 0;
                }
            }

        }


//        if (result[result.length-1] == -1) {
//            System.out.println("getDataWithTrend()：无法找到长度为"+len+"的子数组，请缩减子数组长度len值或增加data数组的长度！");
//            return null;
//        } else {
//            return result;

        //ArrayList转数组
        double[] resultDouble = new double[resultArray.size()];
        for (int i = 0; i < resultArray.size(); i++) {
            resultDouble[i] = resultArray.get(i);
        }

        //返回结果
        if (resultDouble.length == 0){
            System.out.println("getDataWithTrend()：无法找到长度为"+len+"的子数组，请缩减子数组长度len值或增加data数组的长度！");
            return null;
        } else {
            return resultDouble;
        }

    }

    public static void main(String[] args) throws ParseException {
        DataFitting df = new DataFitting();
        //double[] data = df.getDataByStation("1299A", "2015-03-01 00:00:00", "2015-03-05 00:00:00");
        double[] data = {20,12,15,2,10,50,32,129,29,30,27,86,15,44,287,235,12,9,3,42,3,2,443,6};

        double[] result = df.getDataWithTrend(data,"up",5,5);
        for (double aResult : result) System.out.print(aResult + ", ");


    }

}
