package com.DataProcess.DAO;

import org.springframework.stereotype.Repository;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * 船舶sql查询类
 * 包括从数据库查询要求时空范围内的船舶轨迹的相关方法
 *
 * @author An Yuhang
 * @version 1.0
 * @date 2021 /10/26 11:15
 */
@Repository
public class ShipDAO {
    /**
     * Ship log result set.
     *
     * @return the result set
     */
    public ResultSet queryShipLogTest(){
        ResultSet resultSet = null;
        Statement statement;
        String sql = String.format("SELECT ship.updatetime, ship.mmsi, ship.shiplng, ship.shiplat, ship.shipsog, ship.shipcog, ship.geom " +
                "FROM ship_20200725_log as ship WHERE ST_DWithin(ship.geom, ST_GeographyFromText('point(120.786067 32.011783)'), 2000) " +
                "and ship.updatetime > '2020-07-25 15:00:00' and ship.updatetime < '2020-07-25 15:10:00' order by ship.mmsi, ship.updatetime");
        try{
            statement = PostgreSqlJdbcConnect.creatStatement();
            resultSet = statement.executeQuery(sql);
        }catch (SQLException e){
            e.printStackTrace();
        }
        return resultSet;
    }

    /**
     * Query ship log by circle.
     * Select all ship_log within the circle and from startTime to endTime.
     * Order by mmsi, updatetime after query.
     *
     * @return the result set
     * @param centerLng
     * @param centerLat
     * @param radius
     * @param startTime
     * @param endTime
     */
    public ResultSet queryShipLogByCircle(String centerLng, String centerLat, String radius, String startTime, String endTime) throws SQLException{
        String shipLogName = "ship_20200725_log";
        ResultSet resultSet = null;
        Statement statement;
        String sql = String.format(
                "SELECT ship.updatetime, ship.mmsi, ship.shiplng, ship.shiplat, ship.shipsog, ship.shipcog, ship.geom " +
                "FROM " + shipLogName + " as ship " +
                "WHERE ST_DWithin(ship.geom, ST_GeographyFromText('point(" + centerLng + " " + centerLat + ")'), " + radius + ") " +
                "and ship.updatetime > '" + startTime + "' and ship.updatetime < '" + endTime + "' " +
                "order by ship.mmsi, updatetime");
        statement = PostgreSqlJdbcConnect.creatStatement();
        resultSet = statement.executeQuery(sql);
        return resultSet;
    }

    /**
     * Query ship log by square.
     * Select all ship_log within the square and from startTime to endTime.
     * Order by mmsi, updatetime after query.
     *
     * @param vertexLng the vertex lng
     * @param vertexLat the vertex lat
     * @param length    the length
     * @param width     the width
     * @param startTime the start time
     * @param endTime   the end time
     * @return the result set
     */
    public ResultSet queryShipLogBySquare(double vertexLng, double vertexLat, double length, double width, String startTime, String endTime, String day) throws SQLException{
        String shipLogName = "ship_202007" + day +"_log";
        ResultSet resultSet = null;
        Statement statement;
        double lng1 = Double.valueOf(vertexLng);
        double lat1 = Double.valueOf(vertexLat);
        double lng2 = lng1 + length;
        double lat2 = lat1;
        double lng3 = lng1 + length;
        double lat3 = lat1 + width;
        double lng4 = lng1;
        double lat4 = lat1 + width;
        String sql = String.format(
                "SELECT ship.mmsi, ship.shipstatus, ship.shiplng, ship.shiplat, ship.shipsog, ship.shipcog, ship.updatetime, ship.geom " +
                "FROM " + shipLogName + " as ship " + "WHERE ST_Within(ship.geom, ST_GeographyFromText('polygon((" +
                lng1 + " " + lat1 + ", " + lng2 + " " + lat2 + ", " + lng3 + " " + lat3 + ", " + lng4 + " " + lat4 + ", " + lng1 + " " + lat1 + "))')::geometry) " +
                "and ship.updatetime > '" + startTime + "' and ship.updatetime < '" + endTime + "' " +
                "order by ship.mmsi, updatetime");
        statement = PostgreSqlJdbcConnect.creatStatement();
        resultSet = statement.executeQuery(sql);
        return resultSet;
    }

    public ResultSet queryShipLogBySquare(double vertexLng, double vertexLat, double length, double width, String startTime, String endTime) throws SQLException{
        String day = "25";
        return queryShipLogBySquare(vertexLng, vertexLat, length, width, startTime, endTime, day);
    }

    public ResultSet queryShipLogBySquare(Statement statement, double vertexLng, double vertexLat, double length, double width, String startTime, String endTime, String day) throws SQLException{
        String shipLogName = "ship_202007" + day +"_log";
        ResultSet resultSet = null;
        double lng1 = vertexLng;
        double lat1 = vertexLat;
        double lng2 = lng1 + length;
        double lat2 = lat1;
        double lng3 = lng1 + length;
        double lat3 = lat1 + width;
        double lng4 = lng1;
        double lat4 = lat1 + width;
        String sql = String.format(
                "SELECT ship.mmsi, ship.shipstatus, ship.shiplng, ship.shiplat, ship.shipsog, ship.shipcog, ship.updatetime, ship.geom " +
                        "FROM " + shipLogName + " as ship " + "WHERE ST_Within(ship.geom, ST_GeographyFromText('polygon((" +
                        lng1 + " " + lat1 + ", " + lng2 + " " + lat2 + ", " + lng3 + " " + lat3 + ", " + lng4 + " " + lat4 + ", " + lng1 + " " + lat1 + "))')::geometry) " +
                        "and ship.updatetime > '" + startTime + "' and ship.updatetime < '" + endTime + "' " + "and ship.shipsog > 1" +
                        "order by ship.mmsi, updatetime");
        resultSet = statement.executeQuery(sql);
        return resultSet;
    }

    /**
     * Main.
     *
     * @param args the args
     */
    public static void main(String[] args) throws SQLException {
        ShipDAO shipDAO = new ShipDAO();
        ResultSet rs = shipDAO.queryShipLogByCircle("122.180827", "29.931483", "2000", "2020-07-25 15:00:00", "2020-07-25 15:10:00");
        try{
            rs.last();
            rs.getRow();
            rs.beforeFirst();
        } catch (SQLException e){
            e.printStackTrace();
        }
        shipDAO.queryShipLogBySquare(118.059502, 24.473143, 0.02, 0.02, "2020-07-25 15:00:00", "2020-07-25 15:10:00");
    }
}
