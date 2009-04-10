package gov.usgs.vdx.data.tilt;

import gov.usgs.util.ConfigFile;
import gov.usgs.util.Util;
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
import java.util.Date;

/**
 * SQL data source for tilt data
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

	/**
	 * Default constructor
	 */
	public SQLTiltStationDataSource(){}
	/**
	 * Get data source type, "tilt" for this class
	 */
	public String getType()
	{

		return "tilt";
	}
	
	/**
	 * Initialize settings from configuration file
	 */
	public void initialize(ConfigFile params)
	{
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

	/**
	 * Check if database exists.
	 */
	public boolean databaseExists()
	{
		return defaultDatabaseExists(name + "$" + DATABASE_NAME);
	}
	
	/**
	 * Create database for tilt data
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

	/**
	 * Create channel
	 * @param channel channel code
	 * @param channelName channel name
	 * @param lon longitude
	 * @param lat latitude
	 * @return true if success
	 */	
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

	/**
	 * Get channels list in format "sid:code:name:lon:lat" from database
	 */
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

	/**
	 * Get selector name
	 * @param plural if we need selector name in the plural form
	 */
	public String getSelectorName(boolean plural) {
		return plural ? "Stations" : "Station";
	}
	
	/**
	 * Get azimuth for channel
	 * @param code channel code
	 */
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

	public synchronized Date getLastDataTime (String station) {
		Date lastDataTime = null;
        try {
        	database.useDatabase(name + "$" + DATABASE_NAME);
            PreparedStatement ps = database.getPreparedStatement("SELECT max(t) FROM ?");
            ps.setString(1, station);
            ResultSet rs = ps.executeQuery();
            rs.next();
            double result = rs.getDouble(1);
            rs.close();
            lastDataTime = Util.j2KToDate(result);
        } catch (Exception e) {
            e.printStackTrace();
        }
		return lastDataTime;
	}
	
	public double getLastSyncTime (String station) {
		double lastSyncTime = 0.0;
        try {
        	database.useDatabase(name + "$" + DATABASE_NAME);
            PreparedStatement ps = database.getPreparedStatement("SELECT lastSyncTime FROM channels WHERE code = ?");
            ps.setString(1, station);
            ResultSet rs = ps.executeQuery();
            rs.next();
            lastSyncTime = rs.getDouble(1);
            rs.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
		return lastSyncTime;
	}
	
	public void setLastSyncTime (String station, double j2ksec) {
        try {
        	database.useDatabase(name + "$" + DATABASE_NAME);
            PreparedStatement ps = database.getPreparedStatement("UPDATE channels SET lastSyncTime = " + j2ksec + " WHERE code = '" + station + "'");
            ResultSet rs = ps.executeQuery();
            rs.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
	}


	/**
	 * Getter for data. 
	 * Search value of 'action' parameter and retrieve corresponding data:
	 * Possible values are "selectors" and "data".
	 * 
	 * @param command to execute, map of parameter-value pairs.
	 */	
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

	/**
	 * Get tilt data from the database
	 * @param cid channel id
	 * @param st start time
	 * @param et end time
	 */	
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
			while (rs.next()) {
				pts.add(new double[] { rs.getDouble(1), rs.getDouble(2), rs.getDouble(3), rs.getDouble(4), rs.getDouble(5), rs.getDouble(6), rs.getDouble(7), rs.getDouble(8) });
			}
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
	
	/**
	 * Get translation id from database
	 * @param code
	 * @param cx
	 * @param dx
	 * @param cy
	 * @param dy
	 * @param ch
	 * @param dh
	 * @param cb
	 * @param db
	 * @param ci
	 * @param di
	 * @param cg
	 * @param dg
	 * @param cr
	 * @param dr
	 * @param azimuth
	 * @return tid.  -1 if not found
	 */
	public int getTranslation (String code, double cx, double dx, double cy, double dy, double ch, double dh, 
				double cb, double db, double ci, double di, double cg, double dg, double cr, double dr, double azimuth) {
		
		// default the tid as a return value
		int tid = -1;
		PreparedStatement ps;
		ResultSet rs;
		
		// try to lookup the translation in the database
		try {
			database.useDatabase(name + "$" + DATABASE_NAME);
			ps = database.getPreparedStatement("SELECT tid FROM translations " + 
					"WHERE name=? AND cx=? AND dx=? AND cy=? AND dy=? AND ch=? AND dh=? AND cb=? AND db=? " +
					"AND   ci=? AND di=? AND cg=? AND dg=? AND cr=? AND dr=? AND azimuth=?");
			ps.setString(1, code);
			ps.setDouble(2, cx);
			ps.setDouble(3, dx);
			ps.setDouble(4, cy);
			ps.setDouble(5, dy);
			ps.setDouble(6, ch);
			ps.setDouble(7, dh);
			ps.setDouble(8, cb);
			ps.setDouble(9, db);
			ps.setDouble(10,ci);
			ps.setDouble(11,di);
			ps.setDouble(12,cg);
			ps.setDouble(13,dg);
			ps.setDouble(14,cr);
			ps.setDouble(15,dr);
			ps.setDouble(16,azimuth);
			rs = ps.executeQuery();
			if (rs.next()) {
				tid = rs.getInt(1);
			}
			rs.close();
			
		// catch SQLException
		} catch (SQLException e) {
			database.getLogger().log(Level.SEVERE, "SQLTiltStationDataSource.insertTranslation() failed.", e);
		}
		
		return tid;		
	}
	
	/**
	 * Insert translation record into the database, return the new tid, or existing tid if one already exists
	 * @param name
	 * @param cx
	 * @param dx
	 * @param cy
	 * @param dy
	 * @param ch
	 * @param dh
	 * @param cb
	 * @param db
	 * @param ci
	 * @param di
	 * @param cg
	 * @param dg
	 * @param cr
	 * @param dr
	 * @param azimuth
	 * @return tid
	 */
	public int insertTranslation(String code, double cx, double dx, double cy, double dy, double ch, double dh, 
			double cb, double db, double ci, double di, double cg, double dg, double cr, double dr, double azimuth) {
		
		// default local variables
		int tid = -1;
		PreparedStatement ps;
		
		// try to create the translation, if it doesn't exist
		try {
			
			// lookup the translation to see if it exists in the database yet
			tid = getTranslation(code, cx, dx, cy, dy, ch, dh, cb, db, ci, di, cg, dg, cr, dr, azimuth);

			// use the correct database
			database.useDatabase(name + "$" + DATABASE_NAME);
			
			// if the translation does not exist then create a new one
			if (tid == -1) {
				ps = database.getPreparedStatement("INSERT IGNORE INTO translations " +
						"(name, cx, dx, cy, dy, ch, dh, cb, db, ci, di, cg, dg, cr, dr, azimuth) " +
						"VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)");
				ps.setString(1, code);
				ps.setDouble(2, cx);
				ps.setDouble(3, dx);
				ps.setDouble(4, cy);
				ps.setDouble(5, dy);
				ps.setDouble(6, ch);
				ps.setDouble(7, dh);
				ps.setDouble(8, cb);
				ps.setDouble(9, db);
				ps.setDouble(10,ci);
				ps.setDouble(11,di);
				ps.setDouble(12,cg);
				ps.setDouble(13,dg);
				ps.setDouble(14,cr);
				ps.setDouble(15,dr);
				ps.setDouble(16,azimuth);
				ps.execute();
				tid = getTranslation(code, cx, dx, cy, dy, ch, dh, cb, db, ci, di, cg, dg, cr, dr, azimuth);
			}
			
			// update the channels table with the current tid
			ps = database.getPreparedStatement("UPDATE channels SET tid = ? WHERE code = ?");
			ps.setInt(1, tid);
			ps.setString(2, code);
			ps.execute();
			
		// catch SQLException
		} catch (SQLException e) {
			database.getLogger().log(Level.SEVERE, "SQLTiltStationDataSource.insertTranslation() failed.", e);
		}
		
		// return the translation id
		return tid;
	}

	/**
	 * Insert tilt record into the database
	 * @param code channel code
	 * @param t time
	 * @param x x tilt data
	 * @param y y tilt data
	 * @param tid translation id
	 */	
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
	
	public void insertV2Data (String code, double t, double x, double y, double h, double b, double i, double g, double r) {
		try {
			
			// default some variables
			int tid = -1;
			int oid = -1;
			int eid = -1;
			
			// set the database
			database.useDatabase("tilt");
			
			// get the translation and offset
            PreparedStatement ps = database.getPreparedStatement(
            		"SELECT curTrans, curOffset, curEnv FROM stations WHERE code=?");
            ps.setString(1, code);
            ResultSet rs = ps.executeQuery();
            if (rs.next()) {
            	tid = rs.getInt(1);
            	oid = rs.getInt(2);
            	eid = rs.getInt(3);
            }
            rs.close();
            
            // lower case the code because that's how the table names are in the database
            code.toLowerCase();

            // create the tilt entry
            ps = database.getPreparedStatement("INSERT IGNORE INTO " + code + "tilt VALUES (?,?,?,?,?,?,?,?)");
			ps.setDouble(1, t);
			ps.setString(2, Util.j2KToDateString(t));
			ps.setDouble(3, x);
			ps.setDouble(4, y);
			ps.setDouble(5, g);
			ps.setDouble(6, tid);
			ps.setDouble(7, oid);
			ps.setDouble(8, 0);
			ps.execute();
			
			// create the environment entry
            ps = database.getPreparedStatement("INSERT IGNORE INTO " + code + "env VALUES (?,?,?,?,?,?,?,?)");
			ps.setDouble(1, t);
			ps.setString(2, Util.j2KToDateString(t));
			ps.setDouble(3, h);
			ps.setDouble(4, b);
			ps.setDouble(5, i);
			ps.setDouble(6, r);
			ps.setDouble(7, g);
			ps.setDouble(8, eid);
			ps.execute();
			
		} catch (SQLException e) {
			database.getLogger().log(Level.SEVERE, "SQLTiltStationDataSource.insertV2Data() failed.", e);
		}		
	}
}
