package gov.usgs.vdx.data.generic;

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

import cern.colt.matrix.DoubleMatrix2D;

/**
 * 
 * $Log: not supported by cvs2svn $
 * Revision 1.9  2008/04/12 01:17:47  tparker
 * Allow both vdx.name and vdx.databaseName
 *
 * Revision 1.8  2008/03/06 00:24:08  tparker
 * maybe unstable?
 *
 * Revision 1.7  2007/07/24 23:00:17  tparker
 * add support for data insert
 *
 * Revision 1.6  2007/07/24 17:51:22  tparker
 * rename getColumns and expand default time shortcuts
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
public class SQLGenericDataSource extends SQLDataSource implements DataSource
{
	private static final String DATABASE_NAME = "generic";
	
	private List<GenericColumn> columns;
	private List<String> columnStrings;
	private Map<String, String> metadata;
	private String querySQL;
	private String vdxName;
	private String vdxPrefix;
	
	public void initialize(ConfigFile params)
	{
		String vdxHost = params.getString("vdx.host");
		vdxName = params.getString("vdx.name");
		if (vdxName == null || vdxName.equals(""))
			vdxName = params.getString("vdx.databaseName");
		setName(vdxName);
		vdxPrefix = params.getString("vdx.prefix");
		database = new VDXDatabase("com.mysql.jdbc.Driver", "jdbc:mysql://" + vdxHost + "/?user=vdx&password=vdx", vdxPrefix);
	}

	private void getMetadata()
	{
		if (metadata != null)
			return;
		try
		{
			metadata = new HashMap<String, String>();
			database.useDatabase(vdxName + "$" + DATABASE_NAME);
			ResultSet rs = database.getStatement().executeQuery("SELECT meta_key, meta_value FROM metadata");
			while (rs.next())
				metadata.put(rs.getString(1), rs.getString(2));
			rs.close();
		}
		catch (SQLException e)
		{
			database.getLogger().log(Level.SEVERE, "SQLGenericDataSource.getColumns()", e);
		}
	}
	
	private void queryColumnData()
	{
		if (columns != null)
			return;
		try
		{
			columns = new ArrayList<GenericColumn>();
			columnStrings = new ArrayList<String>();
			Statement st = database.getStatement();
			database.useDatabase(vdxName + "$" + DATABASE_NAME);
			ResultSet rs = st.executeQuery("SELECT idx, name, description, unit, checked FROM cols ORDER BY idx ASC");
			while (rs.next())
			{
				GenericColumn col = new GenericColumn();
				col.index = rs.getInt(1);
				col.name = rs.getString(2);
				col.description = rs.getString(3);
				col.unit = rs.getString(4);
				col.checked = rs.getInt(5) == 1;
				columns.add(col);
				columnStrings.add(col.toString());
			}
			rs.close();
			StringBuilder sb = new StringBuilder(256);
			sb.append("SELECT t,");
			for (int i = 0; i < columns.size(); i++)
			{
				GenericColumn col = columns.get(i);
				sb.append(col.name);
				if (i + 1 != columns.size())
					sb.append(",");
			}
			sb.append(" FROM [table] WHERE t>=? AND t<=? ORDER BY t ASC");
			querySQL = sb.toString();
		}
		catch (SQLException e)
		{
			database.getLogger().log(Level.SEVERE, "SQLGenericDataSource.queryColumnData()", e);
		}
	}
	
	public boolean createDatabase()
	{
		try
		{
			String db = vdxName + "$" + DATABASE_NAME;
			if (!createDefaultDatabase(db, 0, true, false))
				return false;
			
			Statement st = database.getStatement();
			database.useDatabase(db);
			st.execute(
					"CREATE TABLE cols (idx INT PRIMARY KEY," +
					"name VARCHAR(255) UNIQUE," +
					"description VARCHAR(255)," + 
					"unit VARCHAR(255)," +
					"checked TINYINT)");
			st.execute(
					"CREATE TABLE metadata (mid INT PRIMARY KEY AUTO_INCREMENT," +
					"meta_key VARCHAR(255)," +
					"meta_value VARCHAR(255))");
			return true;
		}
		catch (SQLException e)
		{
			database.getLogger().log(Level.SEVERE, "SQLGenericDataSource.createDatabase() failed.", e);
		}
		return false;
	}

	public boolean createChannel(String channel, String channelName, double lon, double lat)
	{
		queryColumnData();
		String[] cols = new String[columns.size()];
		for (int i = 0; i < cols.length; i++)
			cols[i] = columns.get(i).name;
		
		return createDefaultChannel(vdxName + "$" + DATABASE_NAME, cols.length, channel, channelName, lon, lat, cols, true, false);
	}
	
	public boolean databaseExists()
	{
		return defaultDatabaseExists(vdxName + "$" + DATABASE_NAME);
	}

	public String getType()
	{
		return "generic";
	}

	public List<String> getSelectors()
	{
		return defaultGetSelectors(DATABASE_NAME);
	}

	public String getSelectorName(boolean plural)
	{
		return plural ? "Stations" : "Station";
	}

	public String getSelectorString()
	{
		String ss = metadata.get("selectorString");
		if (ss == null)
			return "Channels";
		else
			return ss;
	}
	
	public String getDescription()
	{
		String d = metadata.get("description");
		if (d == null)
			return "no description";
		else
			return d;
	}
	
	public String getTitle()
	{
		String t = metadata.get("title");
		if (t == null)
			return "Generic Data";
		else
			return t;
	}
	
	public String getTimeShortcuts()
	{
		String ts = metadata.get("timeShortcuts");
		if (ts == null)
			return "-6h,-24h,-3d,-1w,-1m,-1y";
		else
			return ts;
	}
	
	public RequestResult getData(Map<String, String> params)
	{
		String action = params.get("action");
		if (action == null)
			return null;

		queryColumnData();
		getMetadata();
		
		if (action.equals("genericMenu"))
		{
			List<String> result = new ArrayList<String>(columnStrings.size() + 5);
			result.add(getTitle());
			result.add(getDescription());
			result.add(getSelectorString());
			result.add(getTimeShortcuts());
			result.add(Integer.toString(columnStrings.size()));
			for (String s : columnStrings)
				result.add(s);
			return new TextResult(result);
		}
		else if (action.equals("selectors"))
		{
			List<String> s = getSelectors();
			return new TextResult(s);
		}
		else if (action.equals("data"))
		{
			int cid = Integer.parseInt(params.get("cid"));
			double st = Double.parseDouble(params.get("st"));
			double et = Double.parseDouble(params.get("et"));
			GenericDataMatrix data = getGenericData(cid, st, et);
			if (data != null)
				return new BinaryResult(data);
		}
		return null;
	}

	public GenericDataMatrix getGenericData(int cid, double st, double et)
	{
		GenericDataMatrix result = null;
		try
		{
			database.useDatabase(vdxName + "$" + DATABASE_NAME);
			PreparedStatement ps = database.getPreparedStatement("SELECT code FROM channels WHERE sid=?");
			ps.setInt(1, cid);
			ResultSet rs = ps.executeQuery();
			rs.next();
			String code = rs.getString(1);
			rs.close();

			String sql = querySQL.replace("[table]", code);
			
			ps = database.getPreparedStatement(sql);
			ps.setDouble(1, st);
			ps.setDouble(2, et);
			rs = ps.executeQuery();
			List<double[]> pts = new ArrayList<double[]>();
			while (rs.next())
			{
				double[] d = new double[columns.size() + 1];
				for (int i = 0; i < columns.size() + 1; i++)
					d[i] = rs.getDouble(i + 1);
				pts.add(d);
			}
			rs.close();
			
			if (pts.size() > 0)
				result = new GenericDataMatrix(pts);
		}
		catch (Exception e)
		{
			database.getLogger().log(Level.SEVERE, "SQLGenericDataSource.getGenericData()", e);
		}
		return result;
	}
	
	public void insertData(String table, GenericDataMatrix d)
	{
		String[] colNames = d.getColumnNames();
		DoubleMatrix2D data = d.getData();

		database.useDatabase(vdxName + "$" + DATABASE_NAME);
		Statement st = database.getStatement();

		for (int i=0; i<d.rows(); i++)
		{
			StringBuffer names = new StringBuffer();
			StringBuffer values = new StringBuffer();
			for (int j=0; j<colNames.length; j++)
			{
				names.append(colNames[j] + ",");
				values.append(data.getQuick(i, j) + ",");
			}
			names.deleteCharAt(names.length()-1);
			values.deleteCharAt(values.length()-1);
			
			StringBuffer sql = new StringBuffer();
			sql.append("INSERT IGNORE INTO " + table + " (" + names + ") ");
			sql.append("VALUES (" + values + ")");
			//System.out.println(sql);
			
			try
			{
				st.execute(sql.toString());
			}
			catch (SQLException e)
			{
				database.getLogger().log(Level.SEVERE, "SQLGenericDataSource.insertData() failed.", e);
			}
		}
	}
	public List<GenericColumn> getColumns()
	{
		return columns;
	}
}