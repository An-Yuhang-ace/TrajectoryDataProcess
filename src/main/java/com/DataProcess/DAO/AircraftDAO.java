package com.DataProcess.DAO;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * 航空sql查询类
 * 航空查询函数可以查询一段时间内终端内的航班轨迹
 *
 * @author An Yuhang
 * @version 1.0
 * @date 2022/2/23 20:44
 */
public class AircraftDAO {

    String TERMINAL_AREA_COORDINATE = "116.1166667 39.60833333, 116.0916667 38.98333333, 117 39.23444444, 117 39.96, 117.05 40.09555556, 117.2819444 40.71388889, 116.5666667 40.77777778, 116.5519444 40.60277778, 116.5097222 40.54638889, 116.4166667 40.42388889, 115.5305556 40.41666667, 115.4166667 39.66666667, 116.1166667 39.60833333";

    public ResultSet queryAircraftLogTest(){
        ResultSet resultSet = null;
        Statement statement;
        String sql = String.format("SELECT f.updatetime_t, f.callsign, f.flightlng, f.flightlat, f.height, f.groundspeed, f.heading, f.geometry from flight_20181001_log as f\n" +
                "where ST_Within(f.geometry, ST_GeographyFromText('polygon((" + TERMINAL_AREA_COORDINATE + "))')::geometry)\n" +
                "and f.updatetime_t > '2018-10-01 14:00:00' and f.updatetime_t < '2018-10-01 14:20:00'\n" +
                "order by f.callsign, f.updatetime_t");
        try{
            statement = PostgreSqlJdbcConnect.creatStatement();
            resultSet = statement.executeQuery(sql);
        }catch (SQLException e){
            e.printStackTrace();
        }
        return resultSet;
    }

    public ResultSet queryAircraftLog(String startTime, String endTime, String date){
        ResultSet resultSet = null;
        Statement statement;
        String sql = String.format("SELECT f.callsign, f.updatetime_t, f.flightlng, f.flightlat, f.height, f.groundspeed, f.heading, f.geometry " +
                "from flight_" + date + "_log as f\n" +
                "where ST_Within(f.geometry, ST_GeographyFromText('polygon((" + TERMINAL_AREA_COORDINATE + "))')::geometry)\n" +
                "and f.updatetime_t > '" + startTime + "' and f.updatetime_t < '" + endTime + "'\n" +
                "order by f.callsign, f.updatetime_t");
        try{
            statement = PostgreSqlJdbcConnect.creatStatement();
            resultSet = statement.executeQuery(sql);
        }catch (SQLException e){
            e.printStackTrace();
        }
        return resultSet;
    }

    public ResultSet queryAircraftLog(Statement statement, String startTime, String endTime, String date){
        ResultSet resultSet = null;
        String sql = String.format("SELECT f.callsign, f.updatetime_t, f.flightlng, f.flightlat, f.height, f.groundspeed, f.heading, f.geometry " +
                "from flight_" + date + "_log as f\n" +
                "where ST_Within(f.geometry, ST_GeographyFromText('polygon((" + TERMINAL_AREA_COORDINATE + "))')::geometry)\n" +
                "and f.updatetime_t > '" + startTime + "' and f.updatetime_t < '" + endTime + "'\n" +
                "order by f.callsign, f.updatetime_t");
        try{
            resultSet = statement.executeQuery(sql);
        }catch (SQLException e){
            e.printStackTrace();
        }
        return resultSet;
    }

    public static void main(String[] args){
        AircraftDAO aircraftDAO = new AircraftDAO();
        ResultSet r = aircraftDAO.queryAircraftLog("20181001","2018-10-01 14:00:00", "2018-10-01 14:20:00");
    }


}
