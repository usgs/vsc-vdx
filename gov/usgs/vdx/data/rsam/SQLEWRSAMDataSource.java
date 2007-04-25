package gov.usgs.vdx.data.rsam;

import gov.usgs.util.ConfigFile;
import gov.usgs.vdx.data.DataSource;
import gov.usgs.vdx.data.GenericDataMatrix;
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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;

/**
 * 
 * $Log: not supported by cvs2svn $
 * Revision 1.1  2007/04/22 06:42:26  tparker
 * Initial ewrsam commit
 *
 * Revision 1.5  2006/04/09 18:26:05  dcervelli
 * ConfigFile/type safety changes.
 *
 * Revision 1.4  2005/11/04 18:50:28  dcervelli
 * Fixed bug where columns and metadata not loaded before query.
 *
 * Revision 1.3  2005/10/21 21:19:55  tparker
 * Roll back changes related to Bug #77
 *
 * Revision 1.1  2005/10/20 05:07:30  dcervelli
 * Initial commit.
 *
 * @author Dan Cervelli
 */
public class SQLEWRSAMDataSource extends SQLDataSource implements DataSource
{
	private static final String DATABASE_NAME = "ewrsam";
	
	private RSAMData rd;
	private String querySQL;
	
	public void initialize(ConfigFile params)
	{
		String vdxHost = params.getString("vdx.host");
		String vdxName = params.getString("vdx.name");
		name = params.getString("vdx.databaseName");
		database = new VDXDatabase("com.mysql.jdbc.Driver", "jdbc:mysql://" + vdxHost + "/?user=vdx&password=vdx", vdxName);
	}
	
	public boolean createDatabase()
	{
			String db = name + "$" + DATABASE_NAME;
			return (createDefaultDatabase(db, 0, true, false));
	}

	public boolean createChannel(String channel, String channelName, double lon, double lat)
	{

		String[] cols = new String[2];
		cols[0] = "t";
		cols[1] = "d";
		
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
System.out.println("Getting data for " + cid);
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
}
