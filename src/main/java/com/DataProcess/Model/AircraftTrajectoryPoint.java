package com.DataProcess.Model;

import java.sql.ResultSet;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * 航空轨迹点类表示单个航迹点
 *
 * @author An Yuhang
 * @version 1.0
 * @date 2022/2/23 20:45
 */
public class AircraftTrajectoryPoint extends TrajectoryPoint {

    public String callsign;

    public AircraftTrajectoryPoint(String input){
        super(input);
        this.callsign = this.id;
    }

    public AircraftTrajectoryPoint(ResultSet rs){
        try{
            this.date = df.parse(rs.getString("updatetime_t"));
            this.time = date.getTime() / 1000;
            this.callsign = rs.getString("callsign");
            this.id = this.callsign;
            this.lng = rs.getDouble("flightlng");
            this.lat = rs.getDouble("flightlat");
            this.alt = rs.getDouble("height") * 10;
            this.sog = rs.getDouble("groundspeed");
            this.cog = rs.getDouble("heading");
        } catch (Exception e){
            e.printStackTrace();
        }

    }


}
