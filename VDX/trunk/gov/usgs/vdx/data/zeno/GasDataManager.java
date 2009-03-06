package gov.usgs.vdx.data.zeno;

import java.sql.*;
import java.util.*;
import gov.usgs.util.Util;

/**
 * Customized DatabaseDataManager to connect to one particular database ('gas').
 * Also contains methods to store and request CO2 and SO2 data.
 * 
 * @author  cervelli
 */
public class GasDataManager extends DatabaseDataManager
{
    private static GasDataManager dataManager;

    private GasDataManager() 
    {
        super();
        driver = "org.gjt.mm.mysql.Driver";
        urlAddress = "jdbc:mysql://db_write.internal.hvo/";
        databaseName = "gas";
        parameters = "?user=datauser&password=datauser&autoReconnect=true";
        connect();
    }    
    
    /**
     * Factory method of class
     */
    public static GasDataManager getDataManager()
    {
        if (dataManager == null)
            dataManager = new GasDataManager();
        return dataManager;
    }
    
    /**
     * Adds SO2 data record
     * @return true if record successfully inserted
     */
    public boolean addSO2Data(double j2ksec, int sid, String date, double avg, double min, double max,
            double ws, double wd, double wdsd, double temp, double hum, double press,
            double cal, double volt)
    {
        try
        {
            String sql = "INSERT INTO so2 (j2ksec, sid, time, avgso2, minso2, maxso2, ws, wd, wdsd, temp, hum, press, cal, volt)" +
                    " VALUES (" + j2ksec + "," + sid + ",'" + date + "'," + avg + "," + min + "," + max + "," +
                    ws + "," + wd + "," + wdsd + "," + temp + "," + hum + "," + press + "," + cal + "," + volt + ")";
            statement.execute(sql);
        }
        catch (Exception e)
        {
            if (e.getMessage().indexOf("Duplicate entry") == -1)
            {
                e.printStackTrace();
                return false;
            }
        }
        return true;
    }
    
    /**
     * Adds CO2 data record
     * @return true if record successfully inserted
     */
    public boolean addCO2Data(double j2ksec, int sid, String date, double val)
    {
        try
        {
            String sql = "INSERT INTO co2 (j2ksec, sid, time, co2) VALUES (" + 
                j2ksec + "," + sid + ",'" + date + "'," + val + ")";
            statement.execute(sql);
        }
        catch (Exception e)
        {
            if (e.getMessage().indexOf("Duplicate entry") == -1)
            {
                e.printStackTrace();
                return false;
            }
        }
        return true;
    }

    /**
     * Adds CO2 high data record
     * @return true if record successfully inserted
     */
    public boolean addCO2highData(double j2ksec, int sid, String date, double val)
    {
        try
        {
            String sql = "INSERT INTO co2high (j2ksec, sid, time, co2high) VALUES (" +
                j2ksec + "," + sid + ",'" + date + "'," + val + ")";
            statement.execute(sql);
        }
        catch (Exception e)
        {
            if (e.getMessage().indexOf("Duplicate entry") == -1)
            {
                e.printStackTrace();
                return false;
            }
        }
        return true;
    }

    /**
     * Get list of SO2 stations (array of code-name pairs)
     */
    public String[][] getSO2Stations()
    {
        String[][] stations = null;
        try
        {
            ResultSet rs = statement.executeQuery("SELECT COUNT(*) FROM so2stations");
            rs.next();
            int count = rs.getInt(1);
            rs = statement.executeQuery("SELECT code, name FROM so2stations");
            stations = new String[count][2];
            for (int i = 0; i < count; i++)
            {
                rs.next();
                stations[i][0] = rs.getString(1);
                stations[i][1] = rs.getString(2);
            }
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }
        return stations;
    }
   
    /**
     * Get list of CO2 stations (array of code-name pairs)
     */
    public String[][] getCO2Stations()
    {
        String[][] stations = null;
        try
        {
            ResultSet rs = statement.executeQuery("SELECT COUNT(*) FROM co2stations");
            rs.next();
            int count = rs.getInt(1);
            rs = statement.executeQuery("SELECT code, name FROM co2stations");
            stations = new String[count][2];
            for (int i = 0; i < count; i++)
            {
                rs.next();
                stations[i][0] = rs.getString(1);
                stations[i][1] = rs.getString(2);
            }
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }
        return stations;
    }

    /**
     * Get requested field from so2 table 
     * @param comp field name
     * @param sid sid value to filter output
     * @param t1 minimum j2ksec time
     * @param t2 maximum j2ksec time
     * @return list of time-field_value pairs
     */
    public double[][] getSO2Component(String comp, int sid, double t1, double t2)
    {
        double[][] data = null;
        try
        {
            ResultSet rs = statement.executeQuery("SELECT COUNT(*) FROM so2 WHERE sid=" + sid +
                    " AND j2ksec>=" + t1 + " AND j2ksec<=" + t2);
            int count = 0;
            rs.next();
            count = rs.getInt(1);
            rs = statement.executeQuery("SELECT j2ksec," + comp + " FROM so2 WHERE sid=" + sid +
                    " AND j2ksec>=" + t1 + " AND j2ksec<=" + t2 + " ORDER BY j2ksec");
            data = new double[count][2];
            for (int i = 0; i < count; i++)
            {
                rs.next();
                data[i][0] = rs.getDouble(1);
                data[i][1] = rs.getDouble(2);
            }
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }
        return data;
    }
    
    public double[][] getSO2PPBData(int sid, double t1, double t2)
    {
        double[][] data = null;
        try
        {
            ResultSet rs = statement.executeQuery("SELECT COUNT(*) FROM so2 WHERE sid=" + sid +
                    " AND j2ksec>=" + t1 + " AND j2ksec<=" + t2);
            int count = 0;
            rs.next();
            count = rs.getInt(1);
            rs = statement.executeQuery("SELECT j2ksec, avgso2, maxso2, minso2 FROM so2 WHERE sid=" + sid +
                    " AND j2ksec>=" + t1 + " AND j2ksec<=" + t2 + " ORDER BY j2ksec");
            data = new double[count][4];
            for (int i = 0; i < count; i++)
            {
                rs.next();
                data[i][0] = rs.getDouble(1);
                data[i][1] = rs.getDouble(2);
                data[i][2] = rs.getDouble(3);
                data[i][3] = rs.getDouble(4);
            }
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }
        return data;
    }
    
    public double[][] getCO2PPMData(int sid, double t1, double t2)
    {
        double[][] data = null;
        try
        {
            ResultSet rs = statement.executeQuery("SELECT COUNT(*) FROM co2 WHERE sid=" + sid +
                    " AND j2ksec>=" + t1 + " AND j2ksec<=" + t2);
            int count = 0;
            rs.next();
            count = rs.getInt(1);
            rs = statement.executeQuery("SELECT j2ksec, co2 FROM co2 WHERE sid=" + sid +
                    " AND j2ksec>=" + t1 + " AND j2ksec<=" + t2 + " ORDER BY j2ksec");
            data = new double[count][2];
            for (int i = 0; i < count; i++)
            {
                rs.next();
                data[i][0] = rs.getDouble(1);
                data[i][1] = rs.getDouble(2);
            }
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }
        return data;
    }
    
   /**
    * Dump SO2 data to file 
    * @param fileName to output
    * @param sid sid to filter output
    * @param t1 minimum j2ksec time
    * @param t2 maximum j2ksec time
    */
    public void outputRawSO2Data(String fileName, int sid, double t1, double t2)
    {
        try
        {
            String sql = 
                    "SELECT j2ksec, time, avgso2, minso2, maxso2, ws, wd, wdsd, temp, hum, press, cal, volt " + 
                    "FROM so2 WHERE sid=" + sid + " AND j2ksec>=" + t1 + " AND " +
                    "j2ksec<=" + t2 + " ORDER BY j2ksec ASC";
            ResultSet rs = statement.executeQuery(sql);
            Util.outputData(fileName, "NPS gas data for '" + sid + "'\nj2ksec\tdate\tavgso2\tminso2\tmaxso2\tws\twd\twdsd\ttemp\thum\tpress\tcal\tvolt", rs);
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }
    }
    
    /**
     * Dump CO2 data to file 
     * @param fileName to output
     * @param sid sid to filter output
     * @param t1 minimum j2ksec time
     * @param t2 maximum j2ksec time
     */
    public void outputRawCO2Data(String fileName, int sid, double t1, double t2)
    {
        try
        {
            String sql = 
                    "SELECT j2ksec, time, co2 " + 
                    "FROM co2 WHERE sid=" + sid + " AND j2ksec>=" + t1 + " AND " +
                    "j2ksec<=" + t2 + " ORDER BY j2ksec ASC";
            ResultSet rs = statement.executeQuery(sql);
            Util.outputData(fileName, "CO2 gas data for '" + sid + "'\nj2ksec\tdate\tco2", rs);
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }
    }    
    
    ////////////////////////////////////////////////////////////////////////////
    // These functions are used by outside programs to add data to the database
    ////////////////////////////////////////////////////////////////////////////
    public void addGasData(double j2ksec, int sid, String dstr, int batt, int so2, int co2, int t1deg, int t2deg,
            int wdir, int wspd, int co2hi, int freq, int chksum)
    {
        try
        {
            ResultSet rs = statement.executeQuery("SELECT curmid, curoid FROM gasstations WHERE sid=" + sid);
            rs.next();
            int mid = rs.getInt(1);
            int oid = rs.getInt(2);
            String sql = 
                    "INSERT INTO gasdata (j2ksec, sid, time, batt, so2, co2, t1temp, t2temp, wd, ws, co2hi, freq, chksum, mid, oid) VALUES (" +
                    j2ksec + ", " + sid + ", '" + dstr + "', " + batt + ", " + so2 + ", " + co2 + ", " + t1deg + ", " + 
                    t2deg + ", " + wdir + ", " + wspd + ", " + co2hi + ", " + freq + ", " + chksum + ", " + mid + ", " + oid + ")";
            
            statement.execute(sql);
                    
        }
        catch (Exception e)
        {
            if (e.getMessage().indexOf("Duplicate entry") == -1)
            {
                e.printStackTrace();
            }
        }
    }
    
    public String[] getStationInfo()
    {
        try
        {
            ResultSet rs = statement.executeQuery("SELECT * FROM gasstations");
            Vector v = new Vector();
            while (rs.next())
                v.add(rs.getInt(1) + "\t" + rs.getString(2) + "\t" + rs.getString(3) + "\t" + rs.getDouble(4) + "\t" + rs.getDouble(5) + "\t" + rs.getInt(6) + "\t" + rs.getInt(7));
            
            String[] result = new String[v.size()];
            for (int i = 0; i < v.size(); i++)
                result[i] = (String)v.elementAt(i);
            return result;
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }
        return null;
    }
    
    public boolean addStation(String code, String name, double lon, double lat, int curmid, int curoid)
    {
        try
        {
            statement.execute("INSERT INTO gasstations (code, name, lon, lat, curmid, curoid) VALUES ('" +
                    code + "','" + name + "', " + lon + ", " + lat + ", " + curmid + ", " + curoid + ")");
            return true;
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }
        return false;
    }
    
    public boolean modifyStation(String code, String field, String value)
    {
        try
        {
            statement.execute("UPDATE gasstations SET " + field + "='" + value + "' WHERE code='" + code + "'");
            return true;
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }
        return false;
    }
    
    public String[] getMultInfo()
    {
        try
        {
            ResultSet rs = statement.executeQuery("SELECT * FROM gasmult");
            Vector v = new Vector();
            while (rs.next())
            {
                String line = "";
                for (int i = 1; i <= 12; i++)
                    line = line + rs.getString(i) + "\t";
                
                v.add(line);
            }
            
            String[] result = new String[v.size()];
            for (int i = 0; i < v.size(); i++)
                result[i] = (String)v.elementAt(i);
            return result;
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }
        return null;
    }
    
    public boolean addMult(String comment, double so2, double co2, double co2hi)
    {
        try
        {
            statement.execute("INSERT INTO gasmult (comment, battMult, so2Mult, co2Mult, t1tempMult, t2tempMult, wdMult, wsMult, co2hiMult, freqMult, chksumMult) VALUES ('" +
                    comment + "', 0.0005, " + so2 + ", " + co2 + ", 0.0125, 0.0125, 0.01095, 0.00698, " + co2hi + ", 1 , 1)");
            return true;
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }
        return false;
    }
    
    public boolean modifyMult(String mid, String field, String value)
    {
        try
        {
            statement.execute("UPDATE gasmult SET " + field + "='" + value + "' WHERE mid='" + mid + "'");
            return true;
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }
        return false;
    }
    
    public String[] getOffsetInfo()
    {
        try
        {
            ResultSet rs = statement.executeQuery("SELECT * FROM gasoffset");
            Vector v = new Vector();
            while (rs.next())
            {
                String line = "";
                for (int i = 1; i <= 12; i++)
                    line = line + rs.getString(i) + "\t";
                
                v.add(line);
            }
            
            String[] result = new String[v.size()];
            for (int i = 0; i < v.size(); i++)
                result[i] = (String)v.elementAt(i);
            return result;
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }
        return null;
    }
    
    public boolean addOffset(String comment, double so2, double co2, double co2hi)
    {
        try
        {
            statement.execute("INSERT INTO gasoffset (comment, battOff, so2Off, co2Off, t1tempOff, t2tempOff, wdOff, wsOff, co2hiOff, freqOff, chksumOff) VALUES ('" +
                    comment + "', 0.0005, " + so2 + ", " + co2 + ", 0.0125, 0.0125, 0.01095, 0.00698, " + co2hi + ", 1 , 1)");
            return true;
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }
        return false;
    }
    
    public boolean modifyOffset(String oid, String field, String value)
    {
        try
        {
            statement.execute("UPDATE gasoffset SET " + field + "='" + value + "' WHERE oid='" + oid + "'");
            return true;
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }
        return false;
    }
}