package com.DataProcess.Service;

import com.DataProcess.DAO.ShipDAO;
import com.DataProcess.Model.AircraftTrajectoryPoint;
import com.DataProcess.Model.ShipTrajectoryPoint;
import com.DataProcess.Model.TrajectoryPoint;

import java.io.*;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;

/**
 * 对本地的轨迹文件或者sql查询的轨迹进行插值与对齐，并将处理后的结果写出。
 *
 * @author An Yuhang
 * @version 2.0
 * @date 2022/02/24 15:01
 */
public class InterpolationAndAlignment {

    /**
     * The entry point of application.
     *
     * @param args the input arguments
     * @throws IOException  the io exception
     * @throws SQLException the sql exception
     */
    public static void main(String[] args) throws IOException, SQLException{
        ShipDAO shipDAO = new ShipDAO();
        ResultSet rs = shipDAO.queryShipLogByCircle("122.180827", "29.931483", "2000", "2020-07-25 15:00:00", "2020-07-25 15:10:00");
        shipInterpolateFromSql(rs, "test/test1.csv");
        rs = shipDAO.queryShipLogTest();
        shipInterpolateFromSql(rs, "test/test2.csv");
    }

    /**
     * Interpolate, align and write the ship trajectories from an sql resultSet.
     *
     * @param rs         the sql resultSet
     * @param outputFile the output file
     * @throws SQLException the sql exception
     * @throws IOException  the io exception
     */
    public static void shipInterpolateFromSql(ResultSet rs, String outputFile) throws SQLException, IOException{
        long beginTime = System.currentTimeMillis();
        //Firstly, make sure rs is not null and read the first point.
        if (!rs.next()){
            return;
        }
        List<TrajectoryPoint> trajectory = new ArrayList<>();
        TrajectoryPoint t = new ShipTrajectoryPoint(rs);
        trajectory.add(t);
        //Save the prior point to judge if the next point belongs to the same ship.
        TrajectoryPoint prior = t;
        while(rs.next()){
            //read points in rs and construct the trajectory sequence.
            t = new ShipTrajectoryPoint(rs);
            if (t.id.equals(prior.id)){
                //still the same ship
                //continue reading and processing.
                if (t.time - prior.time > 0){
                    t.deltatime = t.time - prior.time;
                    //t.deltalng = t.lng - prior.lng;
                    //t.deltalat = t.lat - prior.lat;
                    if (t.deltalng == 0.0 && t.deltalat == 0.0) {
                        //经纬度增量都为0，停泊点，时间增量设为0，作为分段点
                        t.deltatime = 0;
                    }
                }
                else if (t.time == prior.time){
                    continue;
                }
                else{
                    trajectory.remove(trajectory.size()-1);
                    prior = trajectory.get(trajectory.size()-1);
                    t.deltatime = t.time - prior.time;
                    //t.deltalng = t.lng - prior.lng;
                    //t.deltalat = t.lat - prior.lat;
                }
                prior = t;
                trajectory.add(t);
            }
            else{
                //trajectory sequence of one ship is constructed
                //then we can interpolate and align the trajectory.
                List<TrajectoryPoint> res = interpolateTrajectory(trajectory);
                shipTrajectoryWrite(res, outputFile);
                trajectory.clear();
                trajectory.add(t);
                prior = t;
            }
        }
        //for the last trajectory
        List<TrajectoryPoint> res = interpolateTrajectory(trajectory);
        shipTrajectoryWrite(res, outputFile);
        trajectory.clear();
        long endTime = System.currentTimeMillis();
        System.out.println(String.format("处理用时%dms", (endTime - beginTime)));
    }

    /**
     * Interpolate, align and write the aircraft trajectories from an sql resultSet.
     *
     * @param rs         the sql resultSet
     * @param outputFile the output file
     * @throws SQLException the sql exception
     * @throws IOException  the io exception
     */
    public static void aircraftInterpolateFromSql(ResultSet rs, String outputFile) throws SQLException, IOException {
        long beginTime = System.currentTimeMillis();
        //Firstly, make sure rs is not null and read the first point.
        if (!rs.next()){
            return;
        }
        List<TrajectoryPoint> trajectory = new ArrayList<>();
        TrajectoryPoint t = new AircraftTrajectoryPoint(rs);
        trajectory.add(t);
        //Save the prior point to judge if the next point belongs to the same ship.
        TrajectoryPoint prior = t;
        while(rs.next()){
            //read points in rs and construct the trajectory sequence.
            t = new AircraftTrajectoryPoint(rs);
            if (t.id.equals(prior.id)){
                //still the same ship
                //continue reading and processing.
                if (t.time - prior.time > 0){
                    t.deltatime = t.time - prior.time;
                    //t.deltalng = t.lng - prior.lng;
                    //t.deltalat = t.lat - prior.lat;
                    if (t.deltalng == 0.0 && t.deltalat == 0.0) {
                        //经纬度增量都为0，停泊点，时间增量设为0，作为分段点
                        t.deltatime = 0;
                    }
                }
                else if (t.time == prior.time){
                    continue;
                }
                else{
                    trajectory.remove(trajectory.size()-1);
                    prior = trajectory.get(trajectory.size()-1);
                    t.deltatime = t.time - prior.time;
                    //t.deltalng = t.lng - prior.lng;
                    //t.deltalat = t.lat - prior.lat;
                }
                prior = t;
                trajectory.add(t);
            }
            else{
                //trajectory sequence of one ship is constructed
                //then we can interpolate and align the trajectory.
                List<TrajectoryPoint> res = interpolateTrajectory(trajectory);
                aircraftTrajectoryWrite(res, outputFile);
                trajectory.clear();
                trajectory.add(t);
                prior = t;
            }
        }
        //for the last trajectory
        List<TrajectoryPoint> res = interpolateTrajectory(trajectory);
        aircraftTrajectoryWrite(res, outputFile);
        trajectory.clear();
        long endTime = System.currentTimeMillis();
        System.out.println(String.format("处理用时%dms", (endTime - beginTime)));
    }

    /**
     * Interpolate, align and write the trajectories from a cvs file.
     *
     * @throws IOException the io exception
     * @param inputFile input file string
     * @param outputFile output file string
     */
    public static void interpolateFromCsv(String inputFile, String outputFile) throws IOException{
        long beginTime = System.currentTimeMillis();
        List<TrajectoryPoint> trajectory = new ArrayList<>();
        //read the csv file
        BufferedReader reader = new BufferedReader((new FileReader(inputFile)));
        String line = reader.readLine();
        ShipTrajectoryPoint t = new ShipTrajectoryPoint(line);
        trajectory.add(t);
        //Save the prior point to judge if the next point belongs to the same ship.
        TrajectoryPoint prior = t;
        while((line = reader.readLine()) != null){
            //read lines in file and construct the trajectory sequence.
            if (line.equals("")){
                continue;
            }
            t = new ShipTrajectoryPoint(line);
            if (t.id.equals(prior.id)){
                //still the same ship
                //continue reading and processing.
                if (t.time - prior.time > 0){
                    t.deltatime = t.time - prior.time;
                    t.deltalng = t.lng - prior.lng;
                    t.deltalat = t.lat - prior.lat;
                    if (t.deltalng == 0.0 && t.deltalat == 0.0) {
                        //经纬度增量都为0，停泊点，时间增量设为0，作为分段点
                        t.deltatime = 0;
                    }
                }
                else if (t.time == prior.time){
                    continue;
                }
                else{
                    trajectory.remove(trajectory.size()-1);
                    prior = trajectory.get(trajectory.size()-1);
                    t.deltatime = t.time - prior.time;
                    t.deltalng = t.lng - prior.lng;
                    t.deltalat = t.lat - prior.lat;
                }
                prior = t;
                trajectory.add(t);
            }
            else{
                //trajectory sequence of one ship is constructed
                //then we can interpolate and align the trajectory.
                List<TrajectoryPoint> a = interpolateTrajectory(trajectory);
                shipTrajectoryWrite(a, outputFile);
                trajectory.clear();
                trajectory.add(t);
                prior = t;
            }
        }
        //for the last trajectory
        List<TrajectoryPoint> a = interpolateTrajectory(trajectory);
        shipTrajectoryWrite(a, outputFile);
        trajectory.clear();
        long endTime = System.currentTimeMillis();
        System.out.println(String.format("处理用时%dms", (endTime - beginTime)));
    }

    /**
     * Interpolate the original trajectory list and generate new trajectory that is aligned temporally.
     *
     * @param trajectory the original trajectory
     * @return new trajectory that is aligned temporally
     */
    private static List<TrajectoryPoint> interpolateTrajectory(List<TrajectoryPoint> trajectory){
        List<TrajectoryPoint> res = new ArrayList<>();
        if (trajectory.size() <= 1){
            return res;
        }
        int index = 0;
        while(index < trajectory.size()-1){
            TrajectoryPoint pre = trajectory.get(index);
            TrajectoryPoint next = trajectory.get(index+1);
            long startTime = pre.time;
            long endTime = next.time;
            long time = startTime / 10 * 10 + 10;
            for (; time <= endTime; time += 10){
                TrajectoryPoint t = linearInterpolate(pre, next, time);
                res.add(t);
            }
            index++;
        }
        return res;
    }


    private static TrajectoryPoint linearInterpolate(TrajectoryPoint pre, TrajectoryPoint next, long time){
        //interpolate linearly and update date and time.
        TrajectoryPoint res = new TrajectoryPoint();
        res.time = time;
        res.setDateWithTime();
        res.id = pre.id;
        res.lng = pre.lng + (next.lng - pre.lng)*(time - pre.time)/(next.time - pre.time);
        res.lat = pre.lat + (next.lat - pre.lat)*(time - pre.time)/(next.time - pre.time);
        res.alt = pre.alt + (next.alt - pre.alt)*(time - pre.time)/(next.time - pre.time);
        res.sog = pre.sog + (next.sog - pre.sog)*(time - pre.time)/(next.time - pre.time);
        res.cog = pre.cog + (next.cog - pre.cog)*(time - pre.time)/(next.time - pre.time);
        return res;
    }


    /**
     * 判断轨迹序列连续段的长度，长度长于阈值 threshold 的轨迹段写入csv文件中
     *
     * @param trajectory the trajectory
     * @param file       the file
     * @throws IOException the io exception
     */
    public static void aircraftTrajectoryWrite(List<TrajectoryPoint> trajectory, String file) throws IOException{
        File csv = new File(file);
        BufferedWriter bw = new BufferedWriter(new FileWriter(csv, true));
        for (int i = 0; i < trajectory.size(); i++){
            TrajectoryPoint node = trajectory.get(i);
            aircraftPointWrite(node, bw);
        }
        bw.close();
    }

    public static void shipTrajectoryWrite(List<TrajectoryPoint> trajectory, String file) throws IOException{
        File csv = new File(file);
        BufferedWriter bw = new BufferedWriter(new FileWriter(csv, true));
        for (int i = 0; i < trajectory.size(); i++){
            TrajectoryPoint node = trajectory.get(i);
            shipPointWrite(node, bw);
        }
        bw.close();
    }

    private static void aircraftPointWrite(TrajectoryPoint a, BufferedWriter bw) throws IOException{
        SimpleDateFormat df = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss");
        String dateString = df.format(a.getDate());
        String callsign = a.getId();
        Double lng = a.getLng();
        Double lat = a.getLat();
        Double alt = a.getAlt();
        Double sog = a.getSog();
        Double cog = a.getCog();
        bw.write(String.format("\"%s\",\"%s\",\"%f\",\"%f\",\"%f\",\"%f\",\"%f\"", dateString, callsign, lng, lat, alt, sog, cog));
        bw.newLine();
    }

    private static void shipPointWrite(TrajectoryPoint a, BufferedWriter bw) throws IOException{
        SimpleDateFormat df = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss");
        String dateString = df.format(a.getDate());
        String mmsi = a.getId();
        Double lng = a.getLng();
        Double lat = a.getLat();
        bw.write(String.format("\"%s\",\"%s\",\"%f\",\"%f\"", dateString, mmsi, lng, lat));
        bw.newLine();
    }

}
