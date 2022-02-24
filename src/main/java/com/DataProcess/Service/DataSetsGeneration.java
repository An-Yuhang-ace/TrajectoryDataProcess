package com.DataProcess.Service;

import com.DataProcess.DAO.AircraftDAO;
import com.DataProcess.DAO.PostgreSqlJdbcConnect;
import com.DataProcess.DAO.ShipDAO;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;

/**
 * Dense ship datasets generation class.
 * Filter dense data in special range and generate the datasets.
 *
 * @author An Yuhang
 * @version 2.0
 * @date 2022/02/24 15:40
 */
public class DataSetsGeneration {
    private static ArrayList<ArrayList<String>> queryTimeList = new ArrayList<>();
    private static final String[] SHIP_LOG_STRING_ARR = new String[]{
            "2020-07-25"
            //, "2020-07-26", "2020-07-27"
    };
    private static final String[] HOUR_STRING_ARR = new String[]{
            "08", "09", "10", "11", "12", "13",
            "14", "15", "16", "17",  "18"
    };
    private static final String[] MIN_STRING_ARR = new String[]{
            "00", "10", "20", "30", "40", "50"
    };
    private static final String[] AIRCRAFT_LOG_STRING_ARR = new String[]{
            "2018-10-01", "2018-10-02", "2018-10-03", "2018-10-04", "2018-10-05", "2018-10-06", "2018-10-07"
    };
    private static final String[] AIRCRAFT_HOUR_STRING_ARR = new String[]{
            "00", "01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "11",
            "12", "13", "14", "15", "16", "17", "18", "19", "20", "21", "22", "23"
    };

    /**
     * The entry point of application.
     *
     * @param arg the input arguments
     * @throws SQLException the sql exception
     * @throws IOException  the io exception
     */
    public static void main(String[] arg) throws SQLException, IOException {
        long beginTime = System.currentTimeMillis();
        //dataSetsFiltering(110.0, 110.1, 20.1, 20.21, 0.1, 0.1, 400);
        //AircraftDAO aircraftDAO = new AircraftDAO();
        //ResultSet rs = aircraftDAO.queryAircraftLog("20181001","2018-10-01 14:00:00", "2018-10-01 14:10:00");
        //InterpolationAndAlignment.aircraftInterpolateFromSql(rs, "dataset/aircraft.csv");
        aircraftDataSetGeneration(300, 0);
        long endTime = System.currentTimeMillis();
        System.out.format("处理用时%ds", (endTime-beginTime)/1000);
    }

    /**
     * Aircraft datasets generation.
     *
     * @param threshold the threshold
     * @param interval  the interval
     * @throws SQLException the sql exception
     * @throws IOException  the io exception
     */
    public static void aircraftDataSetGeneration(int threshold, int interval) throws SQLException, IOException{
        AircraftDAO aircraftDAO = new AircraftDAO();
        if (queryTimeList.isEmpty()){
            // generate the queryTimeList of three days.
            for (String log : AIRCRAFT_LOG_STRING_ARR){
                ArrayList<String> temp = new ArrayList<>();
                for (String hour : AIRCRAFT_HOUR_STRING_ARR){
                    for (String min: MIN_STRING_ARR){
                        temp.add(log + " " + hour + ":" + min + ":00");
                    }
                }
                queryTimeList.add(temp);
            }
        }
        int count = 0;
        Statement statement = PostgreSqlJdbcConnect.creatStatement();
        for (ArrayList<String> timeList : queryTimeList) {
            //Temporal range
            System.out.format("*********** Filtering periods of " + timeList.get(0).substring(0, 10) + " ***********\n");
            for (int i = 0; i < timeList.size() - 1; i++) {
                ResultSet rs = aircraftDAO.queryAircraftLog(statement, timeList.get(i), timeList.get(i + 1), timeList.get(i).substring(0, 4)+timeList.get(i).substring(5, 7)+timeList.get(i).substring(8, 10));
                rs.last();
                int rsSize = rs.getRow();
                String s = timeList.get(i);
                if (rsSize < threshold) {
                    // if size of result < threshold, drop the result set.
                    System.out.format("Points of "
                            + s.substring(8, 10) + timeList.get(i).substring(11, 13) + timeList.get(i).substring(14, 16) + " %d < %d, continue\n", rsSize, threshold);
                    continue;
                }
                rs.beforeFirst();
                //Interpolate, align and output the result.
                System.out.format("Processing "
                        + s.substring(8, 10) + timeList.get(i).substring(11, 13) + timeList.get(i).substring(14, 16) + " ");
                InterpolationAndAlignment.aircraftInterpolateFromSql(rs, "dataset/"
                        + s.substring(8, 10) + timeList.get(i).substring(11, 13) + timeList.get(i).substring(14, 16) + ".csv");
                count++;
            }
        }
            System.out.format("Generated %d dataset more than %d points successfully !", count, threshold);
    }


    /**
     * Dense ship datasets filtering.
     * In geographic range define by (lng1, lng2, lat1, lat2), query the ship data by square in [length, width].
     * If the number of points > threshold, do the interpolation and alignment , then output the dataset.
     *
     * @param lng1      the lng 1
     * @param lng2      the lng 2
     * @param lat1      the lat 1
     * @param lat2      the lat 2
     * @param length    the length
     * @param width     the width
     * @param threshold the threshold
     * @throws SQLException the sql exception
     * @throws IOException  the io exception
     */
    public static void dataSetsFiltering(double lng1, double lng2, double lat1, double lat2, double length, double width, int threshold) throws SQLException, IOException{
        if (lng1 >= lng2){
            System.out.format("INPUT ERROR:lng1%f >= lng2%f", lng1, lng2);
            return;
        }
        if (lat1 >= lat2){
            System.out.format("INPUT ERROR:lat1%f >= lat2%f", lat1, lat2);
            return;
        }
        ShipDAO shipDAO = new ShipDAO();
        if (queryTimeList.isEmpty()){
            // generate the queryTimeList of three days.
            for (String log : SHIP_LOG_STRING_ARR){
                ArrayList<String> temp = new ArrayList<>();
                for (String hour : HOUR_STRING_ARR){
                    for (String min: MIN_STRING_ARR){
                        temp.add(log + " " + hour + ":" + min + ":00");
                    }
                }
                queryTimeList.add(temp);
            }
        }
        int count = 0;
        Statement statement = PostgreSqlJdbcConnect.creatStatement();
        for (double lng = lng1; (lng + length) <= lng2; lng += length){
            for (double lat = lat1; (lat + width) <= lat2; lat += width){
                //Spatial range
                System.out.format("*********** Filtering area of %f, %f ***********\n", lng, lat);
                for (ArrayList<String> timeList : queryTimeList){
                    //Temporal range
                    System.out.format("*********** Filtering periods of 2020-07-" + timeList.get(0).substring(8, 10) + " ***********\n");
                    for (int i = 0; i < timeList.size()-1; i++){
                        ResultSet rs = shipDAO.queryShipLogBySquare(statement, lng, lat, length, width, timeList.get(i), timeList.get(i+1), timeList.get(i).substring(8, 10));
                        rs.last();
                        int rsSize = rs.getRow();
                        String s = timeList.get(i);
                        if (rsSize < threshold){
                            // if size of result < threshold, drop the result set.
                            System.out.format("Points of " + (int)(lng * 100)  + "_" + (int)(lat * 100) + "_"
                                    + s.substring(8,10) + timeList.get(i).substring(11,13) + timeList.get(i).substring(14,16) + " %d < %d, continue\n", rsSize, threshold);
                            continue;
                        }
                        rs.beforeFirst();
                        //Interpolate, align and output the result.
                        System.out.format("Processing " + (int)(lng * 100)  + "_" + (int)(lat * 100) + "_"
                                + s.substring(8,10) + timeList.get(i).substring(11,13) + timeList.get(i).substring(14,16) + " ");
                        InterpolationAndAlignment.shipInterpolateFromSql(rs, "dataset/"
                                + (int)(lng * 100)  + "_" + (int)(lat * 100) + "_"
                                + s.substring(8,10) + timeList.get(i).substring(11,13) + timeList.get(i).substring(14,16) + ".csv");
                        count++;
                    }
                }
            }
        }
        System.out.format("Generated %d dataset more than %d points successfully !", count, threshold);
    }
}
