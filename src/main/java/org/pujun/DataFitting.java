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
            System.out.println(dataSeries.get(i));
        }

        //返回查询到的数据
        return dataArray;

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
            System.out.println(dataSeries.get(i));
        }

        //返回查询到的数据数组
        return dataArray;

    }


    /**
     * 从数组中挖掘出符合特征的子数组
     * @param data 传入的数组
     * @param type 子数组类型 - 上升、下降、峰值、低谷
     * @param len 子数组长度 - 4、8、12、16、20、24、32、40
     * @param num 子数组个数
     */
    public double[] getDataWithTrend(double[] data, String type, int len, int num){
        double[] result = new double[len];

        if (type.equals("up")) {
            int n = 0;
            for (int i = 0; i<data.length; i++) {
                result[n] = data[i];
                double lastResult = data[i];
                //System.out.println(result[num]);
                for (int j = i + 1; j < data.length; j++) {
                    if (data[j] > lastResult && n < 3) {
                        result[++n] = data[j];
                        lastResult = data[j];
                    }
                }
                if (n == 3) break;
                else n = 0;

            }



        } else if (type.equals("down")) {


        } else if (type.equals("peak")) {


        } else if (type.equals("trough")) {

        }
        return result;
    }

    public static void main(String[] args) throws ParseException {
        DataFitting df = new DataFitting();
        //df.getDataByCity("济南市", "2015-03-01 00:00:00", "2015-04-01 00:00:00", 0);
        //df.getDataByStation("1299A", "2015-03-01 09:00:00", "2015-06-01 09:00:00");
        double[] data = {2000,2100,15,2,22,10,50,32,129,30,27,86,15,44,287,235,12,9,3,42,3,2,443,6};
        double[] result = df.getDataWithTrend(data,"up",4,5);
        for (int m = 0; m<result.length;m++){
            System.out.println(result[m]);
        }




//        ArrayList<Double> result = new ArrayList<Double>(4);
//
//        for (int i = 0; i<data.length; i++){
//            result.add(data[i]);
//            double lastResult = data[i];
//            for (int j = i+1; j<data.length;j++){
//                if (data[j] > lastResult && result.size()<4){
//                    result.add(data[j]);
//                    lastResult = data[j];
//                }
//            }
//            if (result.size() == 4){
//                break;
//            }
//        }
//
//
//        double[] resultArray = new double[result.size()];;
//        for (int i = 0; i < result.size(); i++) {
//            resultArray[i] = result.get(i);
//            System.out.println(result.get(i));
//        }

//        double[] result = new double[4];
//        int num = 0;
//        for (int i = 0; i<data.length; i++) {
//            result[num] = data[i];
//            double lastResult = data[i];
//            //System.out.println(result[num]);
//            for (int j = i + 1; j < data.length; j++) {
//                if (data[j] > lastResult && num < 3) {
//                    result[++num] = data[j];
//                    lastResult = data[j];
//                }
//            }
//            if (num == 3) break;
//            else num = 0;
//
//        }
//

    }

}
