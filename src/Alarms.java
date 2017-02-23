package com.shane;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.*;


/**
 * Created by shanesrg on 1/30/2017.
 */
public class Alarms {


    //-- Alarm Model
    private String msh_ts;
    private String alarm_ts;
    private String source;
    private String unit;
    private String bed;
    private String channel;
    private String txt;

/** <p>This method Reads from MSSQL and exports the rows to a <br>
 * Json file on Filesystem.<br>
 * Line 2<br>
 * Line 3<br>
 * Line 4
 * </p>
 *
 *
 * @param filename "Directory and Filename for the Json File to be outputed"
 * @param ssql "The SELECT clause that you want to run"
 * @since     2017-01-01
 *
 *
 *
 * */

    /**
     *
     * @param ssql
     * @param filename
     */


    public void fn_writeJson(String ssql, String filename){
        fn_selectalarms(ssql, filename);

    }

    /**
     *
     * @param obj
     * @param filename
     */
    private void fn_writeToFile(org.json.simple.JSONObject obj, String filename)
    {
        try (FileWriter file = new FileWriter(filename, true)) {

            file.write(obj.toJSONString() + "\n");
            file.flush();

        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    private org.json.simple.JSONObject f_get_alarmJson (){
        org.json.simple.JSONObject obj = new JSONObject();
        obj.put("alarm_ts", getAlarm_ts());
        obj.put("msh_ts", getMsh_ts());
        obj.put("source", getSource());
        obj.put("unit", getUnit());
        obj.put("bed", getBed());
        obj.put("channel", getChannel());
        obj.put("text", getTxt());
        return obj;

    }


    private void fn_selectalarms(String ssql, String filename){

        // Create a variable for the connection string.
        String connectionUrl = "jdbc:sqlserver://ETS-SURFACE:1433;" +
                "databaseName=Golf_App;user=sa;password=bryanbecky";

        // Declare the JDBC objects.
        Connection con = null;
        Statement stmt = null;
        ResultSet rs = null;

        try {
            // Establish the connection.
            Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
            con = DriverManager.getConnection(connectionUrl);

            // Create and execute an SQL statement that returns some data.
            String SQL = ssql;  //"SELECT TOP 1 * FROM alarms_raw";
            stmt = con.createStatement();
            rs = stmt.executeQuery(SQL);

            // Iterate through the data in the result set and display it.
            while (rs.next()) {
                //-- Set Vars
                setAlarm_ts(rs.getString("alarm_ts"));
                setMsh_ts(rs.getString("msh_ts"));
                setSource(rs.getString("source"));
                setUnit(rs.getString("unit"));
                setBed((rs.getString("bed")));
                setChannel(rs.getString("channel"));
                setTxt(rs.getString("text"));

                //-- call f_get_alarmJson () get OBJ BACK
                org.json.simple.JSONObject JsonObj = f_get_alarmJson ();

                //-- Write to FileSystem
                fn_writeToFile(JsonObj, filename);




            }
        }

        // Handle any errors that may have occurred.
        catch (Exception e) {
            e.printStackTrace();
        }
        finally {
            if (rs != null) try { rs.close(); } catch(Exception e) {}
            if (stmt != null) try { stmt.close(); } catch(Exception e) {}
            if (con != null) try { con.close(); } catch(Exception e) {}
        }
    }

    public String getMsh_ts() {
        return msh_ts;
    }

    public void setMsh_ts(String msh_ts) {
        this.msh_ts = msh_ts;
    }

    public String getAlarm_ts() {
        return alarm_ts;
    }

    public void setAlarm_ts(String alarm_ts) {
        this.alarm_ts = alarm_ts;
    }

    public String getSource() {
        return source;
    }

    public void setSource(String source) {
        this.source = source;
    }

    public String getUnit() {
        return unit;
    }

    public void setUnit(String unit) {
        this.unit = unit;
    }

    public String getBed() {
        return bed;
    }

    public void setBed(String bed) {
        this.bed = bed;
    }

    public String getChannel() {
        return channel;
    }

    public void setChannel(String channel) {
        this.channel = channel;
    }

    public String getTxt() {
        return txt;
    }

    public void setTxt(String txt) {
        this.txt = txt;
    }

}




