package gov.usgs.vdx.data.gps;

import gov.usgs.util.ConfigFile;
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

/**
 * 
 * $Log: not supported by cvs2svn $
 * Revision 1.7  2006/04/09 18:26:05  dcervelli
 * ConfigFile/type safety changes.
 *
 * Revision 1.6  2005/10/21 21:23:25  tparker
 * Roll back changes related to Bug #77
 *
 * Revision 1.4  2005/10/19 00:15:13  dcervelli
 * Closed resultsets.
 *
 * Revision 1.3  2005/10/13 20:37:35  dcervelli
 * Checks for already inserted hashes.
 *
 * Revision 1.2  2005/08/29 22:51:36  dcervelli
 * Added insert methods.
 *
 * Revision 1.1  2005/08/26 20:39:00  dcervelli
 * Initial avosouth commit.
 *
 * @author Dan Cervelli
 */
public class SQLGPSDataSource extends SQLDataSource implements DataSource
{
	public static final String DATABASE_NAME = "gps";
	
	/**
	 * Create 'gps' database
	 */
	public boolean createDatabase()
	{
		try
		{
			Statement st = database.getStatement();
			database.useRootDatabase();
			String db = database.getDatabasePrefix() + "_" + name + "$" + DATABASE_NAME;
			st.execute("CREATE DATABASE " + db);
			st.execute("USE " + db);
			st.execute(
					"CREATE TABLE benchmarks (bid INT PRIMARY KEY AUTO_INCREMENT," +
					"code VARCHAR(8) UNIQUE NOT NULL," +
					"name VARCHAR(255), " + 
					"lon DOUBLE, lat DOUBLE, height DOUBLE, " + 
					"btid INT)");
			st.execute(
					"CREATE TABLE benchmark_types (btid INT PRIMARY KEY AUTO_INCREMENT," +
					"name VARCHAR(50))");
			st.execute(
					"CREATE TABLE solution_types (stid INT PRIMARY KEY AUTO_INCREMENT," + 
					"name VARCHAR(100), rank INT)");
			st.execute(
					"CREATE TABLE solutions (sid INT, bid INT," +
					"x DOUBLE, y DOUBLE, z DOUBLE," +
					"sxx DOUBLE, syy DOUBLE, szz DOUBLE," +
					"sxy DOUBLE, sxz DOUBLE, syz DOUBLE," +
					"bad TINYINT(1)," + 
					"PRIMARY KEY (sid, bid))");
			st.execute(
					"CREATE TABLE sources (sid INT AUTO_INCREMENT," +
					"name VARCHAR(255), hash VARCHAR(32)," +
					"t0 DOUBLE NOT NULL, t1 DOUBLE NOT NULL," + 
					"stid INT," +
					"PRIMARY KEY (sid, t0, t1))");
			return true;
		}
		catch (SQLException e)
		{
			database.getLogger().log(Level.SEVERE, "SQLGPSDataSource.createDatabase() failed.", e);
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
	 * Close database connection
	 */
	public void disconnect() {
		database.close();
	}

	/**
	 * Get data source type, "gps" for this class
	 */
	public String getType()
	{
		return "gps";
	}

	/**
	 * Initialize data source from configuration file
	 */
	public void initialize(ConfigFile params)
	{
		String driver = params.getString("vdx.driver");
		String url = params.getString("vdx.url");
		String vdxPrefix = params.getString("vdx.vdxPrefix");
		name = params.getString("vdx.name");
		database = new VDXDatabase(driver, url, vdxPrefix);
	}

	/**
	 * Get list of all benchmarks ordered by code from database
	 */
	public List<Benchmark> getBenchmarks()
	{
		List<Benchmark> result = new ArrayList<Benchmark>();
		try
		{
			database.useDatabase(name + "$" + DATABASE_NAME);
			PreparedStatement ps = database.getPreparedStatement("SELECT bid, code, lon, lat, height FROM benchmarks ORDER BY code");
			ResultSet rs = ps.executeQuery();
			while (rs.next())
			{
				int id = rs.getInt(1);
				String code = rs.getString(2);
				double ln = rs.getDouble(3);
				double lt = rs.getDouble(4);
				double h = rs.getDouble(5);
				Benchmark bm = new Benchmark(id, code, ln, lt, h);
				result.add(bm);
			}
			rs.close();
		}
		catch (Exception e)
		{
			database.getLogger().log(Level.SEVERE, "Could not get benchmark list.");
		}
		return result;
	}
	
	/**
	 * Get list of all solution types from database
	 */
	public List<SolutionType> getSolutionTypes()
	{
		List<SolutionType> result = new ArrayList<SolutionType>();
		try
		{
			database.useDatabase(name + "$" + DATABASE_NAME);
			PreparedStatement ps = database.getPreparedStatement("SELECT stid, name, rank FROM solution_types");
			ResultSet rs = ps.executeQuery();
			while (rs.next())
			{
				int id = rs.getInt(1);
				String name = rs.getString(2);
				int rank = rs.getInt(3);
				SolutionType st = new SolutionType(id, name, rank);
				result.add(st);
			}
			rs.close();
		}
		catch (Exception e)
		{
			database.getLogger().log(Level.SEVERE, "Could not get solution type list.");
		}
		return result;
	}
	
	/**
	 * Get benchmark ID
	 * @param code benchmark code
	 */
	public int getBenchmarkID(String code)
	{
		try
		{
			database.useDatabase(name + "$" + DATABASE_NAME);
			PreparedStatement ps = database.getPreparedStatement("SELECT bid FROM benchmarks WHERE code=?");
			ps.setString(1, code);
			ResultSet rs = ps.executeQuery();
			rs.next();
			int id = rs.getInt(1);
			rs.close();
			return id;
		}
		catch (Exception e)
		{
			database.getLogger().log(Level.SEVERE, "Could not get solution type list.");
		}
		return -1;
	}
	
	/**
	 * Get GPSData
	 * @param code benchmark code
	 * @param stid source id
	 * @param st start time
	 * @param et end time
	 */
	public GPSData getGPSData(String code, int stid, double st, double et)
	{
		int id = getBenchmarkID(code);
		if (id == -1)
			return null;
		else
			return getGPSData(id, stid, st, et);
	}
	
	/**
	 * Get GPSData
	 * @param bid benchmark id
	 * @param stid source id
	 * @param st start time
	 * @param et end time
	 */
	public GPSData getGPSData(int bid, int stid, double st, double et)
	{
		try
		{
			database.useDatabase(name + "$" + DATABASE_NAME);
			List<DataPoint> dataPoints = new ArrayList<DataPoint>();
			String sql = "SELECT (t0+t1)/2, x, y, z, sxx, syy, szz, sxy, sxz, syz FROM solutions " +
					"INNER JOIN benchmarks ON benchmarks.bid=solutions.bid " +
					"INNER JOIN sources ON sources.sid=solutions.sid " +
					"WHERE sources.stid=? AND benchmarks.bid=? " +
					"AND t0>=? AND t1<=? ORDER BY 1";
			PreparedStatement ps = database.getPreparedStatement(sql);
			ps.setInt(1, stid);
//			ps.setString(2, selector);
			ps.setInt(2, bid);
			ps.setDouble(3, st);
			ps.setDouble(4, et);
			ResultSet rs = ps.executeQuery();
			while (rs.next())
			{
				DataPoint dp = new DataPoint();
				dp.t = rs.getDouble(1);
				dp.x = rs.getDouble(2);
				dp.y = rs.getDouble(3);
				dp.z = rs.getDouble(4);
				dp.sxx = rs.getDouble(5);
				dp.syy = rs.getDouble(6);
				dp.szz = rs.getDouble(7);
				dp.sxy = rs.getDouble(8);
				dp.sxz = rs.getDouble(9);
				dp.syz = rs.getDouble(10);
				dataPoints.add(dp);
			}
			rs.close();
			return new GPSData(dataPoints);
		}
		catch (Exception e)
		{
			database.getLogger().log(Level.SEVERE, "Could not get GPS data for " + bid + ", " + st + "->" + et + ".");
		}
		return null;
	}
	
	/**
	 * Getter for data. 
	 * Search value of 'action' parameter and retrieve corresponding data.
	 * @param command to execute.
	 */
	public RequestResult getData(Map<String, String> params)
	{
		String action = params.get("action");
		if (action == null)
			return null;
		
		// TODO: eliminate one or make different
		if (action.equals("selectors"))
		{
			List<String> s = new ArrayList<String>();
			List<Benchmark> bms = getBenchmarks();
			for (Benchmark bm : bms)
				s.add(bm.toFullString());
//				s.add(bm.getLon() + "," + bm.getLat() + ":" + bm.getCode());
			
			return new TextResult(s);
		}
		else if (action.equals("bms"))
		{
			List<String> s = new ArrayList<String>();
			List<Benchmark> bms = getBenchmarks();
			for (Benchmark bm : bms)
				s.add(bm.toFullString());
			
			return new TextResult(s);
		}
		else if (action.equals("data"))
		{
			int bm = Integer.parseInt(params.get("bm"));
			double st = Double.parseDouble(params.get("st"));
			double et = Double.parseDouble(params.get("et"));
			int stid = Integer.parseInt(params.get("stid"));
			GPSData data = getGPSData(bm, stid, st, et);
			if (data != null)
				return new BinaryResult(data);
		}
		return null;
	}

	/**
	 * Insert new benchmark type
	 * @param type name
	 * @return last inserted id or -1 if unsuccessful
	 */
	public int insertBenchmarkType(String n)
	{
		int result = -1;
		try
		{
			database.useDatabase(name + "$" + DATABASE_NAME);
			PreparedStatement ps = database.getPreparedStatement("INSERT INTO benchmark_types (name) VALUES (?)");
			ps.setString(1, n);
			ps.execute();
			ResultSet rs = database.getPreparedStatement("SELECT LAST_INSERT_ID()").executeQuery();
			rs.next();
			result = rs.getInt(1);
			rs.close();
		}
		catch (SQLException e)
		{
			database.getLogger().log(Level.SEVERE, "Could not insert benchmark type.", e);
		}
		return result;
	}
	
	/**
	 * Insert new solution type
	 * @param n type name
	 * @param rank type rank
	 * @return last inserted id or -1 if unsuccessful
	 */
	public int insertSolutionType(String n, int rank)
	{
		int result = -1;
		try
		{
			database.useDatabase(name + "$" + DATABASE_NAME);
			PreparedStatement ps = database.getPreparedStatement("INSERT INTO solution_types (name, rank) VALUES (?,?)");
			ps.setString(1, n);
			ps.setInt(2, rank);
			ps.execute();
			ResultSet rs = database.getPreparedStatement("SELECT LAST_INSERT_ID()").executeQuery();
			rs.next();
			result = rs.getInt(1);
			rs.close();
		}
		catch (SQLException e)
		{
			database.getLogger().log(Level.SEVERE, "Could not insert solution type.", e);
		}
		return result;
	}
	
	public int insertBenchmark(String code, double lon, double lat, double height, int btid)
	{
		int result = -1;
		try
		{
			database.useDatabase(name + "$" + DATABASE_NAME);
			PreparedStatement ps = database.getPreparedStatement("INSERT INTO benchmarks (code, lon, lat, height, btid) VALUES (?,?,?,?,?)");
			ps.setString(1, code);
			ps.setDouble(2, lon);
			ps.setDouble(3, lat);
			ps.setDouble(4, height);
			ps.setInt(5, btid);
			ps.execute();
			ResultSet rs = database.getPreparedStatement("SELECT LAST_INSERT_ID()").executeQuery();
			rs.next();
			result = rs.getInt(1);
			rs.close();
		}
		catch (SQLException e)
		{
			database.getLogger().log(Level.SEVERE, "Could not insert benchmark.", e);
		}
		return result;
	}
	
	public int insertSource(String n, String hash, double t0, double t1, int stid)
	{
		int result = -1;
		try
		{
			database.useDatabase(name + "$" + DATABASE_NAME);
			PreparedStatement ps = database.getPreparedStatement("SELECT COUNT(*) FROM sources WHERE hash=?");
			ps.setString(1, hash);
			ResultSet rs = ps.executeQuery();
			rs.next();
			int cnt = rs.getInt(1);
			rs.close();
			if (cnt > 0)  // already inserted
				return -1;
			ps = database.getPreparedStatement("INSERT INTO sources (name, hash, t0, t1, stid) VALUES (?,?,?,?,?)");
			ps.setString(1, n);
			ps.setString(2, hash);
			ps.setDouble(3, t0);
			ps.setDouble(4, t1);
			ps.setInt(5, stid);
			ps.execute();
			rs = database.getPreparedStatement("SELECT LAST_INSERT_ID()").executeQuery();
			rs.next();
			result = rs.getInt(1);
			rs.close();
		}
		catch (SQLException e)
		{
			database.getLogger().log(Level.SEVERE, "Could not insert source.", e);
		}
		return result;
	}
	
	public void insertSolution(int sid, int bid, DataPoint dp, int bad)
	{
		try
		{
			database.useDatabase(name + "$" + DATABASE_NAME);
			PreparedStatement ps = database.getPreparedStatement("INSERT INTO solutions VALUES (?,?,?,?,?,?,?,?,?,?,?,?)");
			ps.setInt(1, sid);
			ps.setInt(2, bid);
			ps.setDouble(3, dp.x);
			ps.setDouble(4, dp.y);
			ps.setDouble(5, dp.z);
			ps.setDouble(6, dp.sxx);
			ps.setDouble(7, dp.syy);
			ps.setDouble(8, dp.szz);
			ps.setDouble(9, dp.sxy);
			ps.setDouble(10, dp.sxz);
			ps.setDouble(11, dp.syz);
			ps.setInt(12, bad);
			ps.execute();
		}
		catch (SQLException e)
		{
			database.getLogger().log(Level.SEVERE, "Could not insert solution.", e);
		}
	}
}
