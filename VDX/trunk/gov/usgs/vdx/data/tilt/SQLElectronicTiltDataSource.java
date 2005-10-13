package gov.usgs.vdx.data.tilt;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.logging.Level;

/**
 * 
 * $Log: not supported by cvs2svn $
 * @author Dan Cervelli
 */
public class SQLElectronicTiltDataSource extends SQLTiltDataSource
{

	public SQLElectronicTiltDataSource()
	{
		DATABASE_NAME = "etilt";
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
					"cvoltage DOUBLE DEFAULT 1, dvoltage DOUBLE DEFAULT 0," +
					"ctemperature DOUBLE DEFAULT 1, dtemperature DOUBLE DEFAULT 0," +
					"azimuth DOUBLE DEFAULT 0)");
			st.execute("INSERT INTO translations (tid) VALUES (1)");
			return true;
		}
		catch (SQLException e)
		{
			database.getLogger().log(Level.SEVERE, "SQLElectronicTiltDataSource.createDatabase() failed.", e);
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
					"x DOUBLE DEFAULT 0 NOT NULL, y DOUBLE DEFAULT 0 NOT NULL, voltage DOUBLE DEFAULT 0 NOT NULL, temperature DOUBLE DEFAULT 0 NOT NULL, " +
					"tid INT DEFAULT 1 NOT NULL)");
			
			return true;
		}
		catch (SQLException e)
		{
			database.getLogger().log(Level.SEVERE, "SQLElectronicTiltDataSource.createChannel(\"" + channel + "\", " + lon + ", " + lat + ") failed.", e);
		}
		return false;
	}
	
	public void insertData(String code, double t, double x, double y, double v, double temp, double az, double cx, double cy, double dx, double dy, double cv, double dv, double ct, double dt)
	{
		try
		{
			database.useDatabase(name + "$" + DATABASE_NAME);
			PreparedStatement ps = database.getPreparedStatement(
					"SELECT tid FROM translations WHERE cx=? AND cy=? AND dx=? AND dy=? AND azimuth=? AND cvoltage=? AND dvoltage=? AND ctemperature=? AND dtemperature=?");
			ps.setDouble(1, cx);
			ps.setDouble(2, cy);
			ps.setDouble(3, dx);
			ps.setDouble(4, dy);
			ps.setDouble(5, az);
			ps.setDouble(6, cv);
			ps.setDouble(7, dv);
			ps.setDouble(8, ct);
			ps.setDouble(9, dt);
			ResultSet rs = ps.executeQuery();
			int tid = -1;
			if (rs.next())
				tid = rs.getInt(1);
			else
			{
				ps = database.getPreparedStatement(
					"INSERT INTO translations (cx, cy, dx, dy, azimuth, cvoltage, dvoltage, ctemperature, dtemperature) VALUES (?,?,?,?,?,?,?,?,?)");
				ps.setDouble(1, cx);
				ps.setDouble(2, cy);
				ps.setDouble(3, dx);
				ps.setDouble(4, dy);
				ps.setDouble(5, az);
				ps.setDouble(6, cv);
				ps.setDouble(7, dv);
				ps.setDouble(8, ct);
				ps.setDouble(9, dt);
				ps.execute();
				rs = database.getPreparedStatement("SELECT LAST_INSERT_ID()").executeQuery();
				rs.next();
				tid = rs.getInt(1);
			}
			rs.close();
			
			ps = database.getPreparedStatement("INSERT IGNORE INTO " + code + " VALUES (?,?,?,?,?,?)");
			ps.setDouble(1, t);
			ps.setDouble(2, x);
			ps.setDouble(3, y);
			ps.setDouble(4, v);
			ps.setDouble(5, temp);
			ps.setInt(6, tid);
			ps.execute();
		}
		catch (SQLException e)
		{
			database.getLogger().log(Level.SEVERE, "SQLElectronicTiltDataSource.insertData() failed.", e);
		}
	}
}
