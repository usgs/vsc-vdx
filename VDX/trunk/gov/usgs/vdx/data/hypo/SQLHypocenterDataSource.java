package gov.usgs.vdx.data.hypo;

import gov.usgs.util.ConfigFile;
import gov.usgs.vdx.data.DataSource;
import gov.usgs.vdx.data.SQLDataSource;
import gov.usgs.vdx.db.VDXDatabase;
import gov.usgs.vdx.server.BinaryResult;
import gov.usgs.vdx.server.RequestResult;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;

/**
 * 
 * $Log: not supported by cvs2svn $
 * Revision 1.6  2006/04/09 18:26:05  dcervelli
 * ConfigFile/type safety changes.
 *
 * Revision 1.5  2005/10/21 21:23:59  tparker
 * Roll back changes related to Bug #77
 *
 * Revision 1.3  2005/09/21 21:25:00  dcervelli
 * Added ORDER BY to getData().
 *
 * Revision 1.2  2005/09/05 00:42:15  dcervelli
 * Uses PreparedStatements.
 *
 * Revision 1.1  2005/08/26 20:39:00  dcervelli
 * Initial avosouth commit.
 *
 * @author Dan Cervelli
 */
public class SQLHypocenterDataSource extends SQLDataSource implements DataSource
{
	public static final String DATABASE_NAME = "hypocenters";
	public static final String[] DATA_COLUMNS = new String[] {"lon", "lat", "depth", "mag"};
	
	/**
	 * Create 'hypocenters' database
	 */
	public boolean createDatabase()
	{
		String db = name + "$" + DATABASE_NAME;
		if (!createDefaultDatabase(db, DATA_COLUMNS.length, false, false))
			return false;
		
		if (!createDefaultChannel(db, DATA_COLUMNS.length, DATABASE_NAME, null, -1, -1, DATA_COLUMNS, false, false))
			return false;

		try
		{
			database.useDatabase(db);
			database.getStatement().execute("ALTER TABLE " + DATABASE_NAME + " DROP PRIMARY KEY");
			database.getStatement().execute("ALTER TABLE " + DATABASE_NAME + " ADD PRIMARY KEY (j2ksec, lon, lat)");
			return true;
		}
		catch (Exception e)
		{
			logger.log(Level.SEVERE, "Could not create database tables properly.", e);
		}
		return false;
	}

	/**
	 * Get flag if database exist
	 */
	public boolean databaseExists()
	{
		return defaultDatabaseExists(DATABASE_NAME);
	}

	/**
	 * Get data source type, "hypocenters" for this class
	 */
	public String getType()
	{
		return "hypocenters";
	}
	
	/**
	 * Initialize data source from configuration file
	 */
	public void initialize(ConfigFile params)
	{
		String driver = params.getString("vdx.driver");
		String url = params.getString("vdx.url");
		String vdxPrefix = params.getString("vdx.vdxPrefix");
		if (vdxPrefix == null)
			throw new RuntimeException("config parameter vdx.vdxPrefix not found. Update config is using vdx.name");

		name = params.getString("vdx.name");
		if (name == null)
			throw new RuntimeException("config parameter vdx.name not found. Update config if using vdx.databaseName");
		
		database = new VDXDatabase(driver, url, vdxPrefix);
		logger = database.getLogger();
	}
	
	/**
	 * Close database connection
	 */
	public void disconnect() {
		database.close();
	}
	
	/**
	 * Getter for data. 
	 * Search value of following parameters and retrieve corresponding data:
	 * "st" - start time
	 * "et" - end time
	 * "west" - minimum longitude
	 * "east" - maximum longitude
	 * "south" - minimum latitude
	 * "north" -  maximum latitude
	 * "minDepth" - minimum depth
	 * "maxDepth" - maximum depth
	 * "minMag" - minimum magnitude
	 * "maxMag" - maximum magnitude
	 * 
	 * @param command to execute, map of parameter-value pairs.
	 */
	public RequestResult getData(Map<String, String> params)
	{
		try
		{
			double st = Double.parseDouble(params.get("st"));
			double et = Double.parseDouble(params.get("et"));
			double west = Double.parseDouble(params.get("west"));
			double east = Double.parseDouble(params.get("east"));
			double south = Double.parseDouble(params.get("south"));
			double north = Double.parseDouble(params.get("north"));
			double minDepth = Double.parseDouble(params.get("minDepth"));
			double maxDepth = Double.parseDouble(params.get("maxDepth"));
			double minMag = Double.parseDouble(params.get("minMag"));
			double maxMag = Double.parseDouble(params.get("maxMag"));		

			database.useDatabase(name + "$" + DATABASE_NAME);
			// TODO: fix -180/180 wrap
			List<Hypocenter> result = new ArrayList<Hypocenter>();
			String sql = "SELECT j2ksec, lon, lat, depth, mag FROM hypocenters WHERE " +
					"j2ksec>=? AND j2ksec<=? AND lon>=? AND lon<=? AND lat>=? AND lat<=? AND depth>=? AND depth<=? AND mag>=? AND mag<=? " +
					"ORDER BY j2ksec ASC";
			PreparedStatement ps = database.getPreparedStatement(sql);
			ps.setDouble(1, st);
			ps.setDouble(2, et);
			ps.setDouble(3, west);
			ps.setDouble(4, east);
			ps.setDouble(5, south);
			ps.setDouble(6, north);
			ps.setDouble(7, minDepth);
			ps.setDouble(8, maxDepth);
			ps.setDouble(9, minMag);
			ps.setDouble(10, maxMag);
			logger.finest("SQL: " + sql);
			logger.finest("Parameters: " + st + "," + et + "," + west + "," + east + "," + south + "," + north + "," + minDepth + "," + maxDepth + "," + minMag + "," + maxMag);
			ResultSet rs = ps.executeQuery();
			while (rs.next())
			{
				double[] eqd = new double[5];
				eqd[0] = rs.getDouble(1);
				eqd[1] = rs.getDouble(2);
				eqd[2] = rs.getDouble(3);
				eqd[3] = rs.getDouble(4);
				eqd[4] = rs.getDouble(5);
				result.add(new Hypocenter(eqd));
			}
			rs.close();
			logger.finest("Found " + result.size() + " results");
			if (result != null && result.size() > 0)
				return new BinaryResult(new HypocenterList(result));
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
		return null;
	}
	
	/**
	 * Insert hyposenter into database
	 */
	public void insertHypocenter(Hypocenter hc)
	{
		try
		{
			database.useDatabase(name + "$" + DATABASE_NAME);
			String ins = "INSERT IGNORE INTO hypocenters VALUES (?, ?, ?, ?, ?)";
			PreparedStatement ps = database.getPreparedStatement(ins);
			ps.setDouble(1, hc.getTime());
			ps.setDouble(2, hc.getLon());
			ps.setDouble(3, hc.getLat());
			ps.setDouble(4, hc.getDepth());
			ps.setDouble(5, hc.getMag());
			ps.execute();
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
	}
}
