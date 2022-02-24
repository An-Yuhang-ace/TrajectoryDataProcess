package com.DataProcess.Model;

import java.sql.ResultSet;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * The type Trajectory point.
 * 航迹类表示单个航迹点，包括轨迹的时空间信息和时空间增量。
 *
 *
 * @author An Yuhang
 * @version 1.0
 * @date 2021 /1/20 15:33
 */
public class ShipTrajectoryPoint extends TrajectoryPoint {

    public String mmsi;

    public ShipTrajectoryPoint(String input){
        super(input);
        this.mmsi = this.id;
    }

    public ShipTrajectoryPoint(ResultSet rs){
        try{
            this.date = df.parse(rs.getString("updatetime"));
            this.time = date.getTime() / 1000;
            this.mmsi = rs.getString("mmsi");
            this.id = this.mmsi;
            this.lng = rs.getDouble("shiplng");
            this.lat = rs.getDouble("shiplat");
        } catch (Exception e){
            e.printStackTrace();
        }
    }



    public static void main(String[] arg) {
        //test
        String str1 = "27/7/2020 11:10:29";
        String str2 = "27/7/2020 11:10:39";
        SimpleDateFormat df = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss");
        Date date1 = null;
        Date date2 = null;
        try {
            date1 = df.parse(str1);
            date2 = df.parse(str2);
        } catch (Exception e) {
            e.printStackTrace();
        }
        if (date2 != null && date1 != null){
            System.out.println((date2.getTime() - date1.getTime()));
        }
        Date date = new Date(date2.getTime());
    }

}