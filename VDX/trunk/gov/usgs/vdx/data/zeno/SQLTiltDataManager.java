package gov.usgs.vdx.data.zeno;

import java.sql.*;
import gov.usgs.util.Util;

/**
 * This is an SQL implementation of TiltDataManager.
 *
 * <p>SQLTiltDataManager connects through JDBC to a database using the 
 * specified driver and url from the [valvehome]/WEB-INF/config/tilt.config 
 * file.  See that file for what it should contain.
 * 
 * <p>See the data configuration help file, [valvehome]/WEB-INF/doc/config.html
 * for information on the schema required to use this DataManager.
 *
 * $Log: not supported by cvs2svn $
 * 
 * @@author Dan Cervelli
 * @@version 2.00
 */
public class SQLTiltDataManager extends SQLDataManager implements TiltDataManager
{
	/** Configuration file name.
	 */
    protected static final String CONFIG_FILE = "tilt.config";
    
	/** The singleton data manager.
	 */
    protected static SQLTiltDataManager tiltDataManager;
    
	/** The only constructor.  Protected because this class is a singleton.
	 * SQLTiltDataManagers should be acquired through getInstance().
	 * @@param d the database driver
	 * @@param u the database url
	 */
    protected SQLTiltDataManager(String d, String u)
    {
        super();
        driver = d;
        url = u;
        connect();
        System.out.println("SQLTiltDataManager constructor");
    }

	/** Initialization function called by the DataManagerManager.  Reads
	 * the config file and creates a SQLTiltDataManager based on the
	 * driver and url in that file.
	 */
    public static void initialize()
    {
        // ConfigFile cf = new ConfigFile(Valve.getConfigPath() + CONFIG_FILE);
        // initialize(cf.get("driver"), cf.get("url"));
    }
    
	/** Initialization function called by independent non-Valve programs.
	 * Sets the driver and URL manually.
	 * @@param d the driver
	 * @@param u the url
	 */
    public static void initialize(String d, String u)
    {
        if (tiltDataManager == null)
            tiltDataManager = new SQLTiltDataManager(d, u);
    }
	
	/** Gets the singleton SQLTiltDataManager.
	 * @@return the SQLTiltDataManager
	 */
    public static DataManager getInstance()
    {
        return tiltDataManager;
    }
    
	/** Gets the physical lon/lat location of a station.
	 * @@param station the station
	 * @@return double array of length 2, lon/lat
	 */
    public synchronized double[] getLocation(String station)
    {
        double[] result = null;
        try
        {
            ResultSet rs = getStatement().executeQuery("SELECT lon, lat FROM stations WHERE code='" + station + "'");
            if (!rs.next())
                return null;
            result = new double[2];
            result[0] = rs.getDouble(1);
            result[1] = rs.getDouble(2);
        }
        catch (Exception e)
        {
			// Valve.getLogger().warning("Exception: " + e.getMessage());
			System.err.println("Exception: " + e.getMessage());
			if (!inRetry)
				result = (double[])retry("getLocation", new Class[] {String.class}, this, new Object[] {station});
            //e.printStackTrace();
        }
        return result;        
    }
    
	/** Gets a list of strain data stations.
	 * @@return a list of stations (element 0 is id, element 1 is name)
	 */
    public synchronized String[][] getStations()
    {
        String[][] result = null;
        try
        {
            ResultSet rs = getStatement().executeQuery("SELECT COUNT(*) FROM stations");
            rs.next();
            int numStations = rs.getInt(1);
            result = new String[numStations][2];
            rs = getStatement().executeQuery("SELECT code, name FROM stations ORDER BY code ASC");
            for (int i = 0; i < numStations; i++)
            {
                rs.next();
                result[i][0] = rs.getString(1);
                result[i][1] = rs.getString(2);
            }
        }
        catch (Exception e)
        {
            //e.printStackTrace();
			// Valve.getLogger().warning("Exception: " + e.getMessage());
			System.err.println("Exception: " + e.getMessage());
			if (!inRetry)
				result = (String[][])retry("getStations", null, this, null);
        }
        return result;
    }
    
    /** Gets tilt data for a given time and station.
	 * @@param station the station
	 * @@param t1 the start time
	 * @@param t2 the end time
	 * @@return the tilt data
	 */
    public synchronized TiltData getTiltData(String station, double t1, double t2)
    {
        TiltData td = null;
        try
        {
            ResultSet rs = getStatement().executeQuery("SELECT COUNT(*) FROM " + station + "tilt WHERE j2ksec>=" + t1 + 
                    " AND j2ksec<=" + t2);
            if (!rs.next())
                return null;
            int dataCount = rs.getInt(1);
            rs.close();
            
            String sql = 
                    "SELECT j2ksec, " +  
                    "cos(RADIANS(theta))*((x*cx)-xoffset)+sin(RADIANS(theta))*((y*cy)-yoffset) AS east, " +
                    "-sin(RADIANS(theta))*((x*cx)-xoffset)+cos(RADIANS(theta))*((y*cy)-yoffset) AS north " +  
                    "FROM " + station + "tilt INNER JOIN translations ON trans=tid INNER JOIN offsets ON offsetid=oid " + 
                    "WHERE j2ksec>=" + t1 + " AND j2ksec<=" + t2 + " ORDER BY j2ksec";
            rs = getStatement().executeQuery(sql);
            td = new TiltData(dataCount, rs);
            if (td.rows() == 0)
                td = null;
        }
        catch (SQLException e)
        {
			// Valve.getLogger().warning("Exception: " + e.getMessage());
			System.err.println("Exception: " + e.getMessage());
			if (!inRetry)
				td = (TiltData)retry("getTiltData", new Class[] {String.class, Double.TYPE, Double.TYPE}, this,
						new Object[] {station, new Double(t1), new Double(t2)});
            //e.printStackTrace();
        }
        return td;
    }
    
	/** Gets tilt data at the extreme edges of a given time interval and station.
	 * Used for optimization purposes in generating tilt vectors.
	 * @@param station the station
	 * @@param t1 the start time
	 * @@param t2 the end time
	 * @@return the tilt data
	 */
    public synchronized TiltData getTiltDataEdges(String station, double t1, double t2)
    {
        TiltData td = null;
        try
        {
            ResultSet rs = getStatement().executeQuery("SELECT max(j2ksec), min(j2ksec) FROM " + station + 
                    "tilt WHERE j2ksec>=" + t1 + " AND j2ksec<=" + t2);
            if (!rs.next())
                return null;
            double max = rs.getDouble(1);
            double min = rs.getDouble(2);
            if (max == min)
                return null;
            String sql = 
                    "SELECT j2ksec, " +  
                    "cos(RADIANS(theta))*((x*cx)-xoffset)+sin(RADIANS(theta))*((y*cy)-yoffset) AS east, " +
                    "-sin(RADIANS(theta))*((x*cx)-xoffset)+cos(RADIANS(theta))*((y*cy)-yoffset) AS north " +  
                    "FROM " + station + "tilt INNER JOIN translations ON trans=tid INNER JOIN offsets ON offsetid=oid " + 
                    "WHERE j2ksec=" + max + " OR j2ksec=" + min + " ORDER BY j2ksec";
            rs = getStatement().executeQuery(sql);
            td = new TiltData(2, rs);
            if (td.rows() == 0)
                td = null;
        }
        catch (Exception e)
        {
			// Valve.getLogger().warning("Exception: " + e.getMessage());
			System.err.println("Exception: " + e.getMessage());
			if (!inRetry)
				td = (TiltData)retry("getTiltDataEdges", new Class[] {String.class, Double.TYPE, Double.TYPE}, this, 
						new Object[] {station, new Double(t1), new Double(t2)});
            //e.printStackTrace();
        }
        return td;
    }
    
	/** Gets the nominal azimuth to the source of inflation/deflation.
	 * @@param station the station
	 * @@return the nominal azimuth in degrees
	 */
    public synchronized double getNominalAzimuth(String station)
    {
		double result = -1.0;
        try
        {
            ResultSet rs = getStatement().executeQuery("SELECT nom FROM stations WHERE code='" + station + "'");
            if (rs.next())
				result = rs.getDouble(1);
        }
        catch (Exception e)
        {
            //e.printStackTrace();
			// Valve.getLogger().warning("Exception: " + e.getMessage());
			System.err.println("Exception: " + e.getMessage());
			if (!inRetry)
				result = ((Double)retry("getNominalAzimuth", new Class[] {String.class}, this, new Object[] {station})).doubleValue();
        }
        return result;
    }
    
	/** Outputs raw tilt data to a file.
	 * @@param filename output filename
	 * @@param station the station
	 * @@param t1 the start time
	 * @@param t2 the end time
	 */
    public synchronized void outputRawData(String filename, String station, double t1, double t2)
    {
        try
        {
            String sql =
                    "SELECT j2ksec, time, " +  
                    "cos(RADIANS(theta))*((x*cx)-xoffset)+sin(RADIANS(theta))*((y*cy)-yoffset) AS east, " +
                    "-sin(RADIANS(theta))*((x*cx)-xoffset)+cos(RADIANS(theta))*((y*cy)-yoffset) AS north " +  
                    "FROM " + station + "tilt INNER JOIN translations ON trans=tid INNER JOIN offsets ON offsetid=oid " + 
                    "WHERE j2ksec>=" + t1 + " AND j2ksec<=" + t2 + " ORDER BY j2ksec";
            ResultSet rs = getStatement().executeQuery(sql);
            // Util.outputData(filename, "Tilt\nj2ksec\tdate\teast\tnorth", rs);
        }
        catch (SQLException e)
        {
			// Valve.getLogger().warning("Exception: " + e.getMessage());
			System.err.println("Exception: " + e.getMessage());
			if (!inRetry)
				retry("outputRawData", new Class[] {String.class, String.class, Double.TYPE, Double.TYPE}, this,
						new Object[] {filename, station, new Double(t1), new Double(t2)});
//            e.printStackTrace();
        }
    }
    
	/** Gets tilt environment data for a given time and station.
	 * @@param station the station
	 * @@param t1 the start time
	 * @@param t2 the end time
	 * @@return the tilt environment data
	 */
    public synchronized TiltEnvData getTiltEnvData(String station, double t1, double t2)
    {
        TiltEnvData td = null;
        try
        {
            ResultSet rs = getStatement().executeQuery("SELECT COUNT(*) FROM " + station + "env WHERE j2ksec>=" + t1 + 
                    " AND j2ksec<=" + t2);
            if (!rs.next())
                return null;
            int dataCount = rs.getInt(1);
            rs.close();
            
            String sql = 
                    "SELECT j2ksec, " +  
                    "holetemp*holetempmul, " +  
                    "boxtemp*boxtempmul, " +
                    "voltage*voltagemul, " +  
                    "rain*rainmul " +  
                    "FROM " + station + "env INNER JOIN envtranslations ON " + station + "env.eid=envtranslations.eid " + 
                    "WHERE j2ksec>=" + t1 + " AND j2ksec<=" + t2 + " ORDER BY j2ksec";
            rs = getStatement().executeQuery(sql);
            td = new TiltEnvData(dataCount, rs);
            if (td.rows() == 0)
                td = null;
        }
        catch (SQLException e)
        {
            //e.printStackTrace();
			// Valve.getLogger().warning("Exception: " + e.getMessage());
			System.err.println("Exception: " + e.getMessage());
			if (!inRetry)
				td = (TiltEnvData)retry("getTiltEnvData", new Class[] {String.class, Double.TYPE, Double.TYPE}, this,
						new Object[] {station, new Double(t1), new Double(t2)});
        }
        return td;  
    }
    
	/** Outputs raw tilt environment data to a file.
	 * @@param filename output filename
	 * @@param station the station
	 * @@param t1 the start time
	 * @@param t2 the end time
	 */
    public synchronized void outputRawEnvData(String filename, String station, double t1, double t2)
    {
		try
        {
            String sql = 
                    "SELECT j2ksec, time," +  
                    "holetemp*holetempmul, " +  
                    "boxtemp*boxtempmul, " +
                    "voltage*voltagemul, " +  
                    "rain*rainmul " +  
                    "FROM " + station + "env INNER JOIN envtranslations ON " + station + "env.eid=envtranslations.eid " + 
                    "WHERE j2ksec>=" + t1 + " AND j2ksec<=" + t2 + " ORDER BY j2ksec";
            ResultSet rs = getStatement().executeQuery(sql);
            // Util.outputData(filename, "Tilt Environment for '" + station + "'\nj2ksec\t\tdate\tholetemp\tboxtemp\tvoltage\trainfall", rs);
        }
        catch (SQLException e)
        {
            //e.printStackTrace();
			// Valve.getLogger().warning("Exception: " + e.getMessage());
			System.err.println("Exception: " + e.getMessage());
			if (!inRetry)
				retry("outputRawEnvData", new Class[] {String.class, String.class, Double.TYPE, Double.TYPE}, this,
						new Object[] {filename, station, new Double(t1), new Double(t2)});
        }
    }
    
    // From here on is provided for convenience.  These functions are not part 
    // of the implementation of TiltDataManager
    
    /**
     * Get the last data time for station
     */
    public synchronized java.util.Date getLastDataTime(String station)
    {
        try
        {
            ResultSet rs = getStatement().executeQuery("SELECT max(j2ksec) FROM " + station + "tilt");
            if (rs.next())
                return Util.j2KToDate(rs.getDouble(1));
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }
        return null;
    }
    
    /**
     * Insert record in the station's tilt table
     * @param station station name to select table
     * @return true if insertion successful
     */
    public synchronized void insertTiltData(String station, double j2ksec, String date, double x, double y, double agnd)
    {
        try
        {
            ResultSet rs = getStatement().executeQuery("SELECT curTrans, curOffset FROM stations WHERE code='" + station + "'");
            rs.next();
            int curTrans = rs.getInt(1);
            int curOffset = rs.getInt(2);
            getStatement().execute("INSERT INTO " + station + "tilt (j2ksec, time, x, y, agnd, trans, offsetid) VALUES (" + 
                    j2ksec + ",'" + date + "'," + x + ", " + y + "," + agnd + "," + curTrans + "," + curOffset + ")");
        }
        catch (SQLException ex)
        {
            if (ex.getMessage().indexOf("Duplicate entry") == -1)
                ex.printStackTrace();
        }
    }
    
    /**
     * Insert record in the station's env table
     * @param station station name to select table
     * @return true if insertion successful
     */
    public synchronized void insertTiltEnvData(String station, double j2ksec, String date, double ht, double bt, double v, double r, double agnd)
    {
        try
        {
            ResultSet rs = getStatement().executeQuery("SELECT curEnv FROM stations WHERE code='" + station + "'");
            rs.next();
            int curEnv = rs.getInt(1);
            getStatement().execute("INSERT INTO " + station + "env (j2ksec, time, holetemp, boxtemp, voltage, rain, agnd, eid) VALUES (" + 
                    j2ksec + ",'" + date + "'," + ht + ", " + bt + "," + v + "," + r + "," + agnd + "," + curEnv + ")");
        }
        catch (SQLException ex)
        {
            if (ex.getMessage().indexOf("Duplicate entry") == -1)
                ex.printStackTrace();
        }
    }
}