package gov.usgs.vdx.data.rsam;

import gov.usgs.util.ConfigFile;
import gov.usgs.util.Util;
import gov.usgs.vdx.data.DataSource;
import gov.usgs.vdx.data.SQLDataSource;
import gov.usgs.vdx.data.nwis.DataType;
import gov.usgs.vdx.data.nwis.Station;
import gov.usgs.vdx.db.VDXDatabase;
import gov.usgs.vdx.server.BinaryResult;
import gov.usgs.vdx.server.RequestResult;
import gov.usgs.vdx.server.TextResult;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;

import cern.colt.matrix.DoubleMatrix2D;

/**
 * 
 * $Log: not supported by cvs2svn $
 * Revision 1.2  2007/04/25 08:03:16  tparker
 * cleanup
 *
 * Revision 1.1  2007/04/22 06:42:26  tparker
 * Initial ewrsam commit
 *
 * @author Tom Parker
 */
public class SQLEWRSAMDataSource extends SQLDataSource implements DataSource
{
	private static final String DATABASE_NAME = "ewrsam";
	
	private RSAMData rd;
	private String querySQL;
	
	public void initialize(ConfigFile params)
	{
		String url = params.getString("vdx.url");
		String vdxHost = params.getString("vdx.host");
		String vdxName = params.getString("vdx.name");
		name = params.getString("vdx.databaseName");
		
		if (url == null)
			url = "jdbc:mysql://" + vdxHost + "/?user=vdx&password=vdx";
		
		System.out.println("Connecting to " + url);
		database = new VDXDatabase("com.mysql.jdbc.Driver", url, vdxName);
	}
	
	public boolean createDatabase()
	{
			String db = name + "$" + DATABASE_NAME;
			return (createDefaultDatabase(db, 0, true, false));
	}

	public boolean createChannel(String channel, String channelName, double lon, double lat)
	{

		String[] cols = new String[1];
//		cols[0] = "t";
		cols[0] = "d";
		
		return createDefaultChannel(name + "$" + DATABASE_NAME, cols.length, channel, channelName, lon, lat, cols, true, false);
	}
	
	public boolean databaseExists()
	{
		return defaultDatabaseExists(name + "$" + DATABASE_NAME);
	}

	public String getType()
	{
		return "rsam";
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
		
		System.out.println(params.toString());
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
			int cid = Integer.parseInt(params.get("selector"));
			Double pd = Double.parseDouble(params.get("period"));
			int p = pd.intValue();
			double st = Double.parseDouble(params.get("st"));
			double et = Double.parseDouble(params.get("et"));
			RSAMData data = getEWRSAMData(cid, p, st, et);
			if (data != null)
				return new BinaryResult(data);
		}
		return null;
	}

	public RSAMData getEWRSAMData(int cid, int p, double st, double et)
	{
		RSAMData result = null;
		try
		{
			database.useDatabase(name + "$" + DATABASE_NAME);
			PreparedStatement ps = database.getPreparedStatement("SELECT code FROM channels WHERE sid=?");
			ps.setInt(1, cid);
			ResultSet rs = ps.executeQuery();
			rs.next();
			String code = rs.getString(1);
			rs.close();

			String sql = "SELECT t+?/2,avg(d) FROM " + code + " where t >= ? and t <= ? group by floor(t/?);";
			ps = database.getPreparedStatement(sql);
			ps.setDouble(1, p);
//			ps.setString(2, code);
			ps.setDouble(2, st);
			ps.setDouble(3, et);
			ps.setDouble(4, p);
			rs = ps.executeQuery();
			List<double[]> pts = new ArrayList<double[]>();
			while (rs.next())
			{
				double[] d = new double[2];
				d[0] = rs.getDouble(1);
				d[1] = rs.getDouble(2);
				pts.add(d);
			}
			rs.close();
			
			if (pts.size() > 0)
				result = new RSAMData(pts);
		}
		catch (Exception e)
		{
			database.getLogger().log(Level.SEVERE, "SQLEWRSAMDataSource.getEWRSAMData()", e);
		}
		return result;
	}
	
	public void insertData(String channel, DoubleMatrix2D data, boolean r)
	{
		String dbName = name + "$" + DATABASE_NAME;

		System.out.println("dbName = " + dbName);
		if (! database.tableExists(dbName, channel))
			createChannel(channel, channel, -999, -999);
		
		try
		{
			database.useDatabase(dbName);
			String sql;
			if (r)
				sql = "REPLACE INTO ";
			else
				sql = "INSERT IGNORE INTO ";
			
			sql +=  channel + " (t, d) VALUES (?,?)";
			PreparedStatement ps = database.getPreparedStatement(sql);
			for (int i=0; i < data.rows(); i++)
			{
				ps.setDouble(1, data.getQuick(i, 0));
				ps.setDouble(2, data.getQuick(i, 1));
				ps.execute();
			}
		}
		catch (SQLException e)
		{
			database.getLogger().log(Level.SEVERE, "Could not insert data.", e);
		}
	}
}
