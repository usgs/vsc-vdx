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
 * Revision 1.3  2005/10/18 20:25:43  dcervelli
 * Closed resultsets, changed a typo.
 *
 * Revision 1.2  2005/10/14 20:45:08  dcervelli
 * More development.
 *
 * Revision 1.1  2005/10/13 22:17:51  dcervelli
 * Initial commit.
 *
 * @author Dan Cervelli
 */
public class SQLTiltStationDataSource extends SQLDataSource implements DataSource
{
	public static String DATABASE_NAME = "tilt";

	public SQLTiltStationDataSource(){}
	
	public String getType()	{
		return "tilt";
	}
	
	public void initialize(ConfigFile params) {
		String driver = params.getString("vdx.driver");
		String url = params.getString("vdx.url");
		String vdxPrefix = params.getString("vdx.vdxPrefix");
		if (vdxPrefix == null)
			throw new RuntimeException("config parameter vdx.vdxPrefix not found. Update config if using vdx.name");
		
		name = params.getString("vdx.name");
		if (name == null)
			throw new RuntimeException("config parameter vdx.name not found. Update config if using vdx.databaseName");
		
		database = new VDXDatabase(driver, url, vdxPrefix);
		database.getLogger().info("vdx.name:" + name);
	}

	public boolean databaseExists() {
		return defaultDatabaseExists(name + "$" + DATABASE_NAME);
	}
	
	public boolean createDatabase() {
		try {
			Statement st = database.getStatement();
			database.useRootDatabase();
			String db = database.getDatabasePrefix() + "_" + name + "$" + DATABASE_NAME;
			st.execute("CREATE DATABASE " + db);
			st.execute("USE " + db);
			st.execute(
					"CREATE TABLE channels (sid INT PRIMARY KEY AUTO_INCREMENT," +
					"code VARCHAR(16) UNIQUE," +
					"name VARCHAR(255), " + 
					"lon DOUBLE, " +
					"lat DOUBLE, " +
					"azimuth DOUBLE DEFAULT 0, " +
					"tid INT DEFAULT 1)");
			st.execute(
					"CREATE TABLE translations (tid INT PRIMARY KEY AUTO_INCREMENT," +
					"name VARCHAR(255), " +
					"cx DOUBLE DEFAULT 1, dx DOUBLE DEFAULT 0," +
					"cy DOUBLE DEFAULT 1, dy DOUBLE DEFAULT 0," +
					"ch DOUBLE DEFAULT 1, dh DOUBLE DEFAULT 0," +
					"cb DOUBLE DEFAULT 1, db DOUBLE DEFAULT 0," +
					"ci DOUBLE DEFAULT 1, di DOUBLE DEFAULT 0," +
					"cg DOUBLE DEFAULT 1, dg DOUBLE DEFAULT 0," +
					"cr DOUBLE DEFAULT 1, dr DOUBLE DEFAULT 0," +
					"azimuth DOUBLE DEFAULT 0)");
			st.execute("INSERT INTO translations (tid, name) VALUES (1, 'default')");
			return true;
			
		} catch (SQLException e) {
			database.getLogger().log(Level.SEVERE, "SQLTiltStationDataSource.createDatabase() failed.", e);
		}
		return false;
	}
	
	public boolean createChannel(String channel, String channelName, double lon, double lat, double azimuth, int tid) {
		try {
			Statement st = database.getStatement();
			database.useDatabase(name + "$" + DATABASE_NAME);
			String table = channel;
			st.execute(
					"INSERT INTO channels (code, name, lon, lat, azimuth, tid) VALUES ('" + channel + "','" + channelName + "'," + lon + "," + lat + "," + azimuth + "," + tid + ")");
			st.execute(
					"CREATE TABLE " + table + " (" +
					"t DOUBLE PRIMARY KEY, " +
					"x DOUBLE DEFAULT NULL, " +
					"y DOUBLE DEFAULT NULL, " +
					"h DOUBLE DEFAULT NULL, " +
					"b DOUBLE DEFAULT NULL, " +
					"i DOUBLE DEFAULT NULL, " +
					"g DOUBLE DEFAULT NULL, " +
					"r DOUBLE DEFAULT NULL, " +
					"tid INT DEFAULT 1 NOT NULL)");
			
			return true;
			
		} catch (SQLException e) {
			database.getLogger().log(Level.SEVERE, "SQLTiltStationDataSource.createChannel(\"" + channel + "\", " + lon + ", " + lat + ") failed.", e);
		}
		return false;
	}

	public List<String> getSelectors() {
		try {
			List<String> result = new ArrayList<String>();
			database.useDatabase(name + "$" + DATABASE_NAME);
			ResultSet rs = database.executeQuery("SELECT sid, code, name, lon, lat, azimuth FROM channels ORDER BY code");
			if (rs != null) {
				while (rs.next()) {
					result.add(String.format("%d:%f:%f:%s:%s:%f", rs.getInt(1), rs.getDouble(4), rs.getDouble(5), rs.getString(2), rs.getString(3), rs.getDouble(6)));
				}
			}			
			return result;
		} catch (SQLException e) {
			logger.log(Level.SEVERE, "SQLTiltStationDataSource.getSelectors() failed.", e);
		}
		return null;
	}

	public String getSelectorName(boolean plural) {
		return plural ? "Stations" : "Station";
	}
	
	public double getNominalAzimuth (String code) {
		double result = -1.0;
		try {
			database.useDatabase(name + "$" + DATABASE_NAME);
			PreparedStatement ps = database.getPreparedStatement("SELECT azimuth FROM channels WHERE code=?");
			ps.setString(1, code);
			ResultSet rs = ps.executeQuery();
			rs.next();
			result = rs.getDouble(1);
			rs.close();
		} catch (Exception e) {
			database.getLogger().log(Level.SEVERE, "SQLTiltStationDataSource.getNominalAzimuth() failed.", e);
		}
		return result;
	}
	
	public RequestResult getData(Map<String, String> params) {
		
		String action = params.get("action");
		if (action == null)
			return null;
		
		if (action.equals("selectors")) {
			List<String> s = getSelectors();
			return new TextResult(s);
			
		} else if (action.equals("data")) {
			int cid = Integer.parseInt(params.get("cid"));
			double st = Double.parseDouble(params.get("st"));
			double et = Double.parseDouble(params.get("et"));
			TiltStationData data = getTiltStationData(cid, st, et);
			if (data != null)
				return new BinaryResult(data);
		}
		return null;
	}
	
	public TiltStationData getTiltStationData(int cid, double st, double et) {
		try {
			database.useDatabase(name + "$" + DATABASE_NAME);
			PreparedStatement ps = database.getPreparedStatement("SELECT code FROM channels WHERE sid=?");
			ps.setInt(1, cid);
			ResultSet rs = ps.executeQuery();
			rs.next();
			String code = rs.getString(1);
			rs.close();

			ps = database.getPreparedStatement(
				"SELECT t, " +
				"       COS(RADIANS(azimuth))*(x*cx+dx)+SIN(RADIANS(azimuth))*(y*cy+dy), " +
				"      -SIN(RADIANS(azimuth))*(x*cx+dx)+COS(RADIANS(azimuth))*(y*cy+dy), " + 
				"       h*ch+dh, b*cb+db, i*ci+di, g*cg+dg, r*cr+dr " +
				"FROM " + code + " " +
				"INNER JOIN translations ON " + code + ".tid=translations.tid " +
				"WHERE t>=? AND t<=? " +
				"ORDER BY t ASC");
			
			ps.setDouble(1, st);
			ps.setDouble(2, et);
			rs = ps.executeQuery();
			List<double[]> pts = new ArrayList<double[]>();
			while (rs.next())
				pts.add(new double[] { rs.getDouble(1), rs.getDouble(2), rs.getDouble(3), rs.getDouble(4), rs.getDouble(5), rs.getDouble(6), rs.getDouble(7), rs.getDouble(8) });
			rs.close();
			
			TiltStationData td = null;
			if (pts.size() > 0)
				td = new TiltStationData(pts);
			
			return td;
		} catch (SQLException e) {
			database.getLogger().log(Level.SEVERE, "SQLTiltStationDataSource.getTiltData() failed.", e);
		}
		return null;
	}
	
	public void insertData(String code, double t, double x, double y, double h, double b, double i, double g, double r) {
		try {
			int tid = -1;
			database.useDatabase(name + "$" + DATABASE_NAME);
			PreparedStatement ps = database.getPreparedStatement(
					"SELECT tid FROM channels WHERE code=?");
			ps.setString(1, code);
			ResultSet rs = ps.executeQuery();
			if (rs.next()) {
				tid = rs.getInt(1);
			}
			rs.close();			
			ps = database.getPreparedStatement("INSERT IGNORE INTO " + code + " VALUES (?,?,?,?,?,?,?)");
			ps.setDouble(1, t);
			ps.setDouble(2, x);
			ps.setDouble(3, y);
			ps.setDouble(4, h);
			ps.setDouble(5, b);
			ps.setDouble(6, i);
			ps.setDouble(7, g);
			ps.setDouble(8, r);
			ps.setInt(9, tid);
			ps.execute();
		} catch (SQLException e) {
			database.getLogger().log(Level.SEVERE, "SQLTiltStationDataSource.insertData() failed.", e);
		}
	}
}
