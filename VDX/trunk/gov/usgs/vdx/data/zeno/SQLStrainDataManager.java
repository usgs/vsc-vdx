package gov.usgs.vdx.data.zeno;

import java.sql.*;
/**
 * This is an SQL implementation of StrainDataManager.
 *
 * <p>SQLStrainDataManager connects through JDBC to a database using the 
 * specified driver and url from the [valvehome]/WEB-INF/config/strain.config 
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
public class SQLStrainDataManager extends SQLDataManager implements StrainDataManager
{
	/** Configuration file name.
	 */
    protected static final String CONFIG_FILE = "strain.config";
    
	/** The singleton data manager.
	 */
    protected static SQLStrainDataManager strainDataManager;
    
	/** The only constructor.  Protected because this class is a singleton.
	 * SQLStrainDataManagers should be acquired through getInstance().
	 * @@param d the database driver
	 * @@param u the database url
	 */
    protected SQLStrainDataManager(String d, String u)
    {
        super();
        driver = d;
        url = u;
        connect();
        System.out.println("SQLStrainDataManager constructor");
    }

	/** Initialization function called by the DataManagerManager.  Reads
	 * the config file and creates a SQLStrainDataManager based on the
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
        if (strainDataManager == null)
            strainDataManager = new SQLStrainDataManager(d, u);
    }
    
	/** Gets the singleton SQLStrainDataManager.
	 * @@return the SQLStrainDataManager
	 */
    public static DataManager getInstance()
    {
        return strainDataManager;
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
            //e.printStackTrace();
			// Valve.getLogger().warning("Exception: " + e.getMessage());
			System.err.println("Exception: " + e.getMessage());
			if (!inRetry)
				result = (double[])retry("getLocation", new Class[] {String.class}, this, new Object[] {station});
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
    
	/** Gets strain data for a given time and station.
	 * @@param station the station
	 * @@param t1 the start time
	 * @@param t2 the end time
	 * @@return the gas data
	 */
    public synchronized StrainData getStrainData(String station, double t1, double t2)
    {
        StrainData sd = null;
        try
        {
            ResultSet rs = getStatement().executeQuery("SELECT COUNT(*) FROM " + station + "strain WHERE j2ksec>=" + t1 + 
                    " AND j2ksec<=" + t2);
            if (!rs.next())
                return null;
            int dataCount = rs.getInt(1);
            rs.close();
            
            String sql = 
                    "SELECT j2ksec, " +  
                    "dt01*dt01mul, " +
                    "dt02*dt02mul " +  
                    "FROM " + station + "strain INNER JOIN translations ON trans=tid " + 
                    "WHERE j2ksec>=" + t1 + " AND j2ksec<=" + t2 + " ORDER BY j2ksec";
            rs = getStatement().executeQuery(sql);
            sd = new StrainData(dataCount, rs);
            if (sd.rows() == 0)
                sd = null;
        }
        catch (SQLException e)
        {
            //e.printStackTrace();
			// Valve.getLogger().warning("Exception: " + e.getMessage());
			System.err.println("Exception: " + e.getMessage());
			if (!inRetry)
				sd = (StrainData)retry("getStrainData", new Class[] {String.class, Double.TYPE, Double.TYPE}, this,
						new Object[] {station, new Double(t1), new Double(t2)});
        }
        return sd;
    }
    
	/** Outputs raw strain data to a file.
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
                    "dt01*dt01mul, " +
                    "dt02*dt02mul " +  
                    "FROM " + station + "strain INNER JOIN translations ON trans=tid " + 
                    "WHERE j2ksec>=" + t1 + " AND j2ksec<=" + t2 + " ORDER BY j2ksec";
            ResultSet rs = getStatement().executeQuery(sql);
            // Util.outputData(filename, "Strain for '" + station + "'\nj2ksec\tdate\tdt01\tdt02", rs);
        }
        catch (SQLException e)
        {
            //e.printStackTrace();
			// Valve.getLogger().warning("Exception: " + e.getMessage());
			System.err.println("Exception: " + e.getMessage());
			if (!inRetry)
				retry("outputRawData", new Class[] {String.class, String.class, Double.TYPE, Double.TYPE}, this,
						new Object[] {filename, station, new Double(t1), new Double(t2)});
        }
    }
    
	/** Gets strain environment data for a given time and station.
	 * @@param station the station
	 * @@param t1 the start time
	 * @@param t2 the end time
	 * @@return the strain environment data
	 */
    public synchronized StrainEnvData getStrainEnvData(String station, double t1, double t2)
    {
        StrainEnvData sd = null;
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
                    "bar*barmul, " +
                    "voltage*voltagemul, " +  
                    "rainfall*rainmul " +  
                    "FROM " + station + "env INNER JOIN envtranslations ON trans=eid " + 
                    "WHERE j2ksec>=" + t1 + " AND j2ksec<=" + t2 + " ORDER BY j2ksec";
            rs = getStatement().executeQuery(sql);
            sd = new StrainEnvData(dataCount, rs);
            if (sd.rows() == 0)
                sd = null;
        }
        catch (SQLException e)
        {
            //e.printStackTrace();
			// Valve.getLogger().warning("Exception: " + e.getMessage());
			System.err.println("Exception: " + e.getMessage());
			if (!inRetry)
				sd = (StrainEnvData)retry("getStrainEnvData", new Class[] {String.class, Double.TYPE, Double.TYPE}, this,
						new Object[] {station, new Double(t1), new Double(t2)});
        }
        return sd;  
    }
    
	/** Outputs raw strain environment data to a file.
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
                    "bar*barmul, " +
                    "voltage*voltagemul, " +  
                    "rainfall*rainmul " +  
                    "FROM " + station + "env INNER JOIN envtranslations ON trans=eid " + 
                    "WHERE j2ksec>=" + t1 + " AND j2ksec<=" + t2 + " ORDER BY j2ksec";
            ResultSet rs = getStatement().executeQuery(sql);
            // Util.outputData(filename, "Strain Environment for '" + station + "'\nj2ksec\t\tdate\tholetemp\tpressure\tvoltage\trainfall", rs);
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
    // of the implementation of StrainDataManager
    
    /**
     * Get the last data time for station
     */
    public java.util.Date getLastDataTime(String station)
    {
        try
        {
            ResultSet rs = getStatement().executeQuery("SELECT max(j2ksec) FROM " + station + "strain");
            if (rs.next()) {
                // return Util.j2KToDate(rs.getDouble(1));
            }
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }
        return null;
    }
   
    /**
     * Insert record in the station's strain table
     * @param station station name to select table
     * @return true if insertion successful
     */
    public boolean insertStrainData(String station, double j2ksec, String time, double dt01, double dt02, double agnd)
    {
        try
        {
            ResultSet rs = getStatement().executeQuery("SELECT curTrans FROM stations WHERE code='" + station + "'");
            if (!rs.next())
                return false;
            int curTrans = rs.getInt(1);
            getStatement().execute("INSERT INTO " + station + "strain (j2ksec, time, dt01, dt02, agnd, trans) VALUES (" +
                j2ksec + ",'" + time + "'," + dt01 + "," + dt02 + "," + agnd + "," + curTrans + ")");
            return true;
        }
        catch (Exception e)
        {
            if (e.getMessage().indexOf("Duplicate entry") == -1)
                e.printStackTrace();
        }
        return false;
    }
    
    /**
     * Insert record in the station's env table
     * @param station station name to select table
     * @return true if insertion successful
     */
    public boolean insertStrainEnvData(String station, double j2ksec, String time, double bar, double ht, double v, double r, double agnd)
    {
        try
        {
            ResultSet rs = getStatement().executeQuery("SELECT curEnvTrans FROM stations WHERE code='" + station + "'");
            if (!rs.next())
                return false;
            int curTrans = rs.getInt(1);
            getStatement().execute("INSERT INTO " + station + "env (j2ksec, time, bar, holetemp, voltage, rainfall, agnd, trans) VALUES (" +
                j2ksec + ",'" + time + "'," + bar + "," + ht + "," + v + "," + r + "," + agnd + "," + curTrans + ")");
            return true;
        }
        catch (Exception e)
        {
            if (e.getMessage().indexOf("Duplicate entry") == -1)
                e.printStackTrace();
        }
        return false;
    }
    
}