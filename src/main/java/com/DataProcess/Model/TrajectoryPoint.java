package com.DataProcess.Model;

import java.sql.ResultSet;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * 轨迹点父类，有船舶轨迹点和飞机轨迹点两个子类
 *
 * @author An Yuhang
 * @version 1.0
 * @date 2022/2/24 11:42
 */
public class TrajectoryPoint {
    SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    public String id;
    /** Date格式时间 */
    public Date date;
    /** UTC时间,单位ms */
    public long time;
    /** 时间增量,单位ms。 若 == 0 则代表为轨迹起始点 */
    public long deltatime = 0;
    public double lng, lat, alt;
    public double cog, sog;
    /** 经纬度增量 */
    public double deltalng = 0.0, deltalat = 0.0, deltaalt = 0.0;

    public TrajectoryPoint(String input){
        //根据读取到的轨迹点数据创建一个TrajectoryPoint对象
        input = input.replace("\"","");
        String[] line = input.split(",");
        try{
            this.date = df.parse(line[0]);
        } catch (Exception e) {
            e.printStackTrace();
        }
        this.time = date.getTime() / 1000;
        this.id = line[1];
        this.lng = Double.parseDouble(line[2]);
        this.lat = Double.parseDouble(line[3]);
        this.sog = Double.parseDouble(line[4]);
        this.cog = Double.parseDouble(line[5]);
    }

    public TrajectoryPoint(){
        //创建一个空的TrajectoryPoint对象
    }

    public String getId() {
        return id;
    }
    public Date getDate() {
        return date;
    }
    public long getTime() {
        return time;
    }
    public long getDeltatime() {
        return deltatime;
    }

    public double getLng() {
        return lng;
    }
    public double getLat() {
        return lat;
    }
    public double getAlt() { return alt; }

    public double getDeltalng() {
        return deltalng;
    }
    public double getDeltalat() {
        return deltalat;
    }
    public double getDeltaalt() { return deltaalt; }

    public double getSog() {
        return sog;
    }
    public double getCog() {
        return cog;
    }
    public void setDateWithTime(){
        this.date = new Date(this.time * 1000);
    }

}
