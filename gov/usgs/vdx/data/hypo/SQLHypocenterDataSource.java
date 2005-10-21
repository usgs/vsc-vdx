package gov.usgs.vdx.data.hypo;

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
			database.getStatement().execute("ALTER TABLE " + DATABASE_NAME + " ADD PRIMARY KEY (t, lon, lat)");
			return true;
		}
		catch (Exception e)
		{
			database.getLogger().log(Level.SEVERE, "Could not create database tables properly.", e);
		}
		return false;
	}

	public boolean databaseExists()
	{
		return defaultDatabaseExists(DATABASE_NAME);
	}

	public String getType()
	{
		return "hypocenters";
	}
	
	public void initialize(Map<String, Object> params)
	{
		database = (VDXDatabase)params.get("VDXDatabase");
		if (database == null)
		{
			String vdxHost = (String)params.get("vdx.host");
			String vdxName = (String)params.get("vdx.name");
			params.put("name", (String)params.get("vdx.databaseName"));
			database = new VDXDatabase("com.mysql.jdbc.Driver", "jdbc:mysql://" + vdxHost + "/?user=vdx&password=vdx", vdxName);
		}
		
		name = (String)params.get("name");
	}
	
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
			String sql = "SELECT t, lon, lat, depth, mag FROM hypocenters WHERE " +
					"t>=? AND t<=? AND lon>=? AND lon<=? AND lat>=? AND lat<=? AND depth>=? AND depth<=? AND mag>=? AND mag<=? " +
					"ORDER BY t ASC";
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
			if (result != null && result.size() > 0)
				return new BinaryResult(new HypocenterList(result));
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
		return null;
	}
	
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
