package gov.usgs.vdx.data.rsam;

import gov.usgs.util.ConfigFile;
import gov.usgs.util.Log;
import gov.usgs.vdx.data.DataSource;
import gov.usgs.vdx.data.SQLDataSource;
import gov.usgs.vdx.db.VDXDatabase;
import gov.usgs.vdx.server.BinaryResult;
import gov.usgs.vdx.server.RequestResult;
import gov.usgs.vdx.server.TextResult;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;

import cern.colt.matrix.DoubleMatrix2D;

/**
 * 
 * SQL class for file-based RSAM data
 *
 * @author Tom Parker
 */
public class SQLEWRSAMDataSource extends SQLDataSource implements DataSource
{
	private static final String DATABASE_NAME = "ewrsam";
	
	private String tableSuffix;
	
	/**
	 * Default constructor
	 */
	public SQLEWRSAMDataSource()
	{
		super();
	}
	
	/**
	 * Constructor
	 * @param s data type (Events/Values)
	 */
	public SQLEWRSAMDataSource(String s)
	{
		super();
		
		if (s.equals("Events"))
			tableSuffix = "_events";
		else if (s.equals("Values"))
			tableSuffix = "_values";
	}


	/**
	 * Initialize data source with settings from configuration file
	 */
	public void initialize(ConfigFile params)
	{
		String url = params.getString("vdx.url");
		String driver = params.getString("vdx.driver");
		String vdxPrefix = params.getString("vdx.vdxPrefix");
		name = params.getString("vdx.name");
				
		System.out.println("Connecting to " + url);
		database = new VDXDatabase(driver, url, vdxPrefix);
	}
	
	/**
	 * Create database with name "'vdx.name config parameter'$ewrsam"
	 */
	public boolean createDatabase()
	{
			String db = name + "$" + DATABASE_NAME;
			return (createDefaultDatabase(db, 0, true, false));
	}

	/**
	 * Create channel
	 * @param channel channel code
	 * @param channelName channel name
	 * @param lon longitude
	 * @param lat latitude
	 * @return true if success
	 */
	public boolean createChannel(String channel, String channelName, double lon, double lat)
	{
		if (channelName == null)
			channelName = "";

		try
		{
			Statement st = database.getStatement();
			database.useDatabase(name + "$" + DATABASE_NAME);

			st.execute("INSERT INTO channels (code, name, lon, lat)" +
				" VALUES ('" + channel + "','" + channelName + "'," + 
				lon + "," + lat + ")");
		
			st.execute("CREATE TABLE " + channel + "_values" + 
				" (j2ksec DOUBLE PRIMARY KEY," +
				" d DOUBLE DEFAULT 0 NOT NULL)");

			st.execute("CREATE TABLE " + channel + "_events" + 
				" (j2ksec DOUBLE PRIMARY KEY," +
				" d DOUBLE DEFAULT 0 NOT NULL)");
			
			return true;
		}
		catch (SQLException e)
		{
			database.getLogger().log(Level.SEVERE, "SQLDataSource.createDefaultChannel(\"" + channel + "\", " + lon + ", " + lat + ") failed.", e);
			e.printStackTrace();
		}
		return false;
	}
	
	/**
	 * Check if database exist.
	 */
	public boolean databaseExists()
	{
		return defaultDatabaseExists(name + "$" + DATABASE_NAME);
	}

	/**
	 * Get data source type, "ewrsam" for this class
	 */
	public String getType()
	{
		return "ewrsam";
	}

	/**
	 * Get channels list in format "sid:code:name:lon:lat" from database
	 */
	public List<String> getSelectors()
	{
		return defaultGetSelectors(DATABASE_NAME);
	}

	/**
	 * Search database tables for events and values
	 * @return if values table found, add "VALUES" string to list. if events tables found, add "EVENTS" string to list
	 */
	public List<String> getTypes()
	{
		List<String> result = new ArrayList<String>();
		ResultSet rs;
		
		logger.info("Entering SQLEWRSAMDataSource.getTypes()");
		
		logger.info("SQLEWRSAMDataSource.getTypes() using " + name + "$" + DATABASE_NAME);
		database.useDatabase(name + "$" + DATABASE_NAME);
		rs = database.executeQuery("SHOW TABLES LIKE '%_events'");
		try 
		{
			rs.next();
			logger.info("SQLEWRSAMDataSource.getTypes(): Found events table " + rs.getString(1));
			result.add("EVENTS");
		}
		catch (SQLException e) 
		{
			logger.info("SQLEWRSAMDataSource.getTypes(): No events tables found");
		}
		
		rs = database.executeQuery("SHOW TABLES LIKE '%_values'");
		try
		{
			rs.next();
			logger.info("SQLEWRSAMDataSource.getTypes(): Found values table " + rs.getString(1));
			result.add("VALUES");
		}
		catch (SQLException e)
		{
			logger.info("SQLEWRSAMDataSource.getTypes(): No values tables found ");
		}

		return result;
	}

	/**
	 * Get selector name
	 * @param plural if we need selector name in the plural form
	 */
	public String getSelectorName(boolean plural)
	{
		return plural ? "Stations" : "Station";
	}
	
	/**
	 * Getter for data. 
	 * Search value of 'action' parameters and retrieve corresponding data.
	 * 
	 * Possible values are "data", "selectors", "ewRsamMenu"
	 * 
	 * @param command to execute, map of parameter-value pairs.
	 */
	public RequestResult getData(Map<String, String> params)
	{
		
		logger = Log.getLogger("gov.usgs.vdx");
			
		logger.info("SQLEWRSAMDataSource.getData(): params = " + params.toString());
		String action = params.get("action");
		
		if (action == null)
			action = "data";
		
		if (action.equals("selectors"))
		{
			List<String> s = getSelectors();
			return new TextResult(s);
		}
		else if (action.equals("data"))
		{
			int ch = Integer.parseInt(params.get("selector"));
			Double pd = Double.parseDouble(params.get("period"));
			int p = pd.intValue();
			double st = Double.parseDouble(params.get("st"));
			double et = Double.parseDouble(params.get("et"));
			String type = params.get("type");
			EWRSAMData data = getEWRSAMData(ch, p, type, st, et);
			
			if (data != null)
				return new BinaryResult(data);
		}
		else if (action.equals("ewRsamMenu"))
		{
			List<String> t = getTypes();
			return new TextResult(t);
		}
		else
		{
			logger.info("SQLEWRSAMDataSource.getData(): unknown action " + action);
		}
		logger.info("SQLEWRSAMDataSource.getData(): returning null data");
		return null;
	}

	/**
	 * Get EW RSAM data
	 * @param cid channel code
	 * @param p period
	 * @param type data type (EVENTS/VALUES)
	 * @param st start time
	 * @param et end time
	 */
	public EWRSAMData getEWRSAMData(int cid, int p, String type, double st, double et)
	{
		
		EWRSAMData result = null;
		List<double[]> pts = null;
		List<double[]> events = null;
		
		try
		{
			database.useDatabase(name + "$" + DATABASE_NAME);

			PreparedStatement ps = database.getPreparedStatement("SELECT code FROM channels WHERE sid=?");
			ps.setInt(1, cid);
			ResultSet rs = ps.executeQuery();
			rs.next();
			String code = rs.getString(1);
			rs.close();

			if (type.equals("VALUES"))
			{
				String sql = "SELECT j2ksec+?/2,avg(d) FROM " + code + "_values" + " where j2ksec >= ? and j2ksec <= ? group by floor(j2ksec / ?);";
				ps = database.getPreparedStatement(sql);
				ps.setDouble(1, p);
				ps.setDouble(2, st);
				ps.setDouble(3, et);
				ps.setDouble(4, p);
				rs = ps.executeQuery();
				pts = new ArrayList<double[]>();
				while (rs.next())
				{
					double[] d = new double[2];
					d[0] = rs.getDouble(1);
					d[1] = rs.getDouble(2);
					pts.add(d);
				}
				rs.close();
				System.out.println("Found " + pts.size() + " values");
			}
			else if (type.equals("EVENTS"))
			{
				String sql = "SELECT j2ksec, d FROM " + code + "_events" + " where j2ksec >= ? and j2ksec <= ? and d != 0;";
				ps = database.getPreparedStatement(sql);
				ps.setDouble(1, st);
				ps.setDouble(2, et);
				rs = ps.executeQuery();
				events = new ArrayList<double[]>();
				double count = 0;
	
				double[] d = new double[2];
				d[0] = st;
				d[1] = count;
				events.add(d);
			
				while (rs.next())
				{
					double c = rs.getDouble(2);
					double t = rs.getDouble(1);
					for (int i = 0; i < c; i++)
					{
						d = new double[2];
						d[0] = t;
						d[1] = ++count;
						events.add(d);
					}
				}
				rs.close();
	
				d = new double[2];
				d[0] = et;
				d[1] = count;
				events.add(d);
	
			}
				if (pts.size() > 0 || events.size() > 0)
					result = new EWRSAMData(pts, events);
		}
		catch (Exception e)
		{
			database.getLogger().log(Level.SEVERE, "SQLEWRSAMDataSource.getEWRSAMData()", e);
		}
		return result;
	}
	
	/**
	 * Insert data for channel
	 * 
	 * @param channel channel code
	 * @param data data matrix to insert
	 * @param r if we permit data replacing?
	 */
	public void insertData(String channel, DoubleMatrix2D data, boolean r)
	{
		String dbName = name + "$" + DATABASE_NAME;

		logger.info("SQLEWRSAMDataSource.insertData: dbName = " + dbName);
		if (! database.tableExists(dbName, channel + "_events"))
			createChannel(channel, channel, -999, -999);
		
		try
		{
			database.useDatabase(dbName);
			String sql;
			if (r)
				sql = "REPLACE INTO ";
			else
				sql = "INSERT IGNORE INTO ";
			
			sql +=  channel + tableSuffix + " (j2ksec, d) VALUES (?,?)";
			PreparedStatement ps = database.getPreparedStatement(sql);
			for (int i=0; i < data.rows(); i++)
			{
				if (i % 100 == 0)
					System.out.print(".");
				ps.setDouble(1, data.getQuick(i, 0));
				ps.setDouble(2, data.getQuick(i, 1));
				ps.execute();
			}
			System.out.println();
		}
		catch (SQLException e)
		{
			database.getLogger().log(Level.SEVERE, "Could not insert data.", e);
		}
	}
}