package gov.usgs.vdx.data.tilt;

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
 * Revision 1.9  2005/10/21 21:24:51  tparker
 * Roll back changes related to Bug #77
 *
 * Revision 1.7  2005/10/19 00:14:17  dcervelli
 * Closed resultsets.
 *
 * Revision 1.6  2005/10/14 21:06:38  dcervelli
 * Fixed typo.
 *
 * Revision 1.5  2005/10/13 22:18:02  dcervelli
 * Changes for etilt.
 *
 * Revision 1.4  2005/09/24 17:34:36  dcervelli
 * Returns E/N instead of X/Y.
 *
 * Revision 1.3  2005/09/06 21:35:16  dcervelli
 * Added ORDER BY to getTiltData().
 *
 * Revision 1.2  2005/09/05 20:53:48  dcervelli
 * Continued work on tilt.
 *
 * Revision 1.1  2005/08/26 20:39:00  dcervelli
 * Initial avosouth commit.
 *
 * @author Dan Cervelli
 */
public class SQLTiltDataSource extends SQLDataSource implements DataSource
{
	public static String DATABASE_NAME = "tilt";
	
	public String getType()
	{
		return "tilt";
	}
	
	public void initialize(ConfigFile params)
	{
		String vdxHost = params.getString("vdx.host");
		String vdxName = params.getString("vdx.name");
		name = params.getString("vdx.databaseName");
		database = new VDXDatabase("com.mysql.jdbc.Driver", "jdbc:mysql://" + vdxHost + "/?user=vdx&password=vdx", vdxName);
	}

	public boolean databaseExists()
	{
		return defaultDatabaseExists(name + "$" + DATABASE_NAME);
	}
	
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
					"CREATE TABLE channels (sid INT PRIMARY KEY AUTO_INCREMENT," +
					"code VARCHAR(16) UNIQUE," +
					"name VARCHAR(255), " + 
					"lon DOUBLE, lat DOUBLE)");
			st.execute(
					"CREATE TABLE translations (tid INT PRIMARY KEY AUTO_INCREMENT," +
					"cx DOUBLE DEFAULT 1, cy DOUBLE DEFAULT 1," +
					"dx DOUBLE DEFAULT 0, dy DOUBLE DEFAULT 0," +
					"azimuth DOUBLE DEFAULT 0)");
			st.execute("INSERT INTO translations (tid) VALUES (1)");
			return true;
		}
		catch (SQLException e)
		{
			database.getLogger().log(Level.SEVERE, "SQLTiltDataSource.createDatabase() failed.", e);
		}
		return false;
	}
	
	public boolean createChannel(String channel, String channelName, double lon, double lat)
	{
		try
		{
			Statement st = database.getStatement();
			database.useDatabase(name + "$" + DATABASE_NAME);
			String table = channel;
			st.execute(
					"INSERT INTO channels (code, name, lon, lat) VALUES ('" + channel + "','" + channelName + "'," + lon + "," + lat + ")");
			st.execute(
					"CREATE TABLE " + table + " (t DOUBLE PRIMARY KEY," +
					"x DOUBLE DEFAULT 0 NOT NULL, y DOUBLE DEFAULT 0 NOT NULL," +
					"tid INT DEFAULT 1 NOT NULL)");
			
			return true;
		}
		catch (SQLException e)
		{
			database.getLogger().log(Level.SEVERE, "SQLTiltDataSource.createChannel(\"" + channel + "\", " + lon + ", " + lat + ") failed.", e);
		}
		return false;
	}

	public List<String> getSelectors()
	{
		return defaultGetSelectors(DATABASE_NAME);
	}

	public String getSelectorName(boolean plural)
	{
		return plural ? "Stations" : "Station";
	}

	public RequestResult getData(Map<String, String> params)
	{
		String action = params.get("action");
		if (action == null)
			return null;
		
		if (action.equals("selectors"))
		{
			List<String> s = getSelectors();
			return new TextResult(s);
		}
		else if (action.equals("data"))
		{
			int cid = Integer.parseInt(params.get("cid"));
			double st = Double.parseDouble(params.get("st"));
			double et = Double.parseDouble(params.get("et"));
			TiltData data = getTiltData(cid, st, et);
			if (data != null)
				return new BinaryResult(data);
		}
		return null;
	}
	
	public TiltData getTiltData(int cid, double st, double et)
	{
		try
		{
			database.useDatabase(name + "$" + DATABASE_NAME);
			PreparedStatement ps = database.getPreparedStatement("SELECT code FROM channels WHERE sid=?");
			ps.setInt(1, cid);
			ResultSet rs = ps.executeQuery();
			rs.next();
			String code = rs.getString(1);
			rs.close();
			
			ps = database.getPreparedStatement(
					"SELECT t, " +
					"COS(RADIANS(azimuth))*(x*cx+dx)+SIN(RADIANS(azimuth))*(y*cy+dy)," +
					"-SIN(RADIANS(azimuth))*(x*cx+dx)+COS(RADIANS(azimuth))*(y*cy+dy) FROM " + code +	
					" INNER JOIN translations ON " + code + ".tid=translations.tid" +
					" WHERE t>=? AND t<=? ORDER BY t ASC");
			
			ps.setDouble(1, st);
			ps.setDouble(2, et);
			rs = ps.executeQuery();
			List<double[]> pts = new ArrayList<double[]>();
			while (rs.next())
				pts.add(new double[] { rs.getDouble(1), rs.getDouble(2), rs.getDouble(3) });
			rs.close();
			
			TiltData td = null;
			if (pts.size() > 0)
				td = new TiltData(pts);
			
			return td;
		}
		catch (SQLException e)
		{
			database.getLogger().log(Level.SEVERE, "SQLTiltDataSource.getTiltData() failed.", e);
		}
		return null;
	}
	
	public void insertData(String code, double t, double x, double y, double az, double cx, double cy, double dx, double dy)
	{
		try
		{
			database.useDatabase(name + "$" + DATABASE_NAME);
			PreparedStatement ps = database.getPreparedStatement(
					"SELECT tid FROM translations WHERE cx=? AND cy=? AND dx=? AND dy=? AND azimuth=?");
			ps.setDouble(1, cx);
			ps.setDouble(2, cy);
			ps.setDouble(3, dx);
			ps.setDouble(4, dy);
			ps.setDouble(5, az);
			ResultSet rs = ps.executeQuery();
			int tid = -1;
			if (rs.next())
				tid = rs.getInt(1);
			else
			{
				ps = database.getPreparedStatement(
					"INSERT INTO translations (cx, cy, dx, dy, azimuth) VALUES (?,?,?,?,?)");
				ps.setDouble(1, cx);
				ps.setDouble(2, cy);
				ps.setDouble(3, dx);
				ps.setDouble(4, dy);
				ps.setDouble(5, az);
				ps.execute();
				rs = database.getPreparedStatement("SELECT LAST_INSERT_ID()").executeQuery();
				rs.next();
				tid = rs.getInt(1);
			}
			rs.close();
			
			ps = database.getPreparedStatement("INSERT IGNORE INTO " + code + " VALUES (?,?,?,?)");
			ps.setDouble(1, t);
			ps.setDouble(2, x);
			ps.setDouble(3, y);
			ps.setInt(4, tid);
			ps.execute();
		}
		catch (SQLException e)
		{
			database.getLogger().log(Level.SEVERE, "SQLTiltDataSource.insertData() failed.", e);
		}
	}
}
