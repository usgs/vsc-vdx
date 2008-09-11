package gov.usgs.vdx.data.generic.variable;

import gov.usgs.util.ConfigFile;
import gov.usgs.util.Util;
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
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;

/**
 * 
 * $Log: not supported by cvs2svn $
 * Revision 1.12  2008/04/09 20:09:26  tparker
 * tweak table naming
 *
 * Revision 1.10  2008/04/07 23:14:14  tparker
 * update create channel_data_types
 *
 * Revision 1.9  2007/06/29 07:29:17  tparker
 * Add channel_data_type table
 *
 * Revision 1.8  2007/01/31 00:03:26  tparker
 * Add ingestor for NWIS archive style data
 *
 * Revision 1.7  2007/01/29 21:55:56  tparker
 * Disable filling sparse data. Causes trouble for precip
 *
 * Revision 1.6  2006/09/21 18:41:02  tparker
 * kludge to deal with sparse data
 *
 * Revision 1.5  2006/09/20 23:14:38  tparker
 * Add active column to DB
 *
 * Revision 1.4  2006/09/19 21:41:23  tparker
 * lengthen site_no field
 *
 * Revision 1.3  2006/09/19 18:02:50  tparker
 * tweak db creation name
 *
 * Revision 1.2  2006/08/28 23:58:42  tparker
 * Initial NWIS commit
 *
 * Revision 1.1  2006/08/01 19:54:47  tparker
 * Create NWIS data source
 *
 * 
 * @author Tom Parker
 */
public class SQLNWISDataSource extends SQLDataSource implements DataSource
{
	public static final String DATABASE_NAME = "nwis";
	
	public boolean createDatabase()
	{
		
		try
		{
			Statement st = database.getStatement();
			database.useRootDatabase();
			String db = database.getDatabasePrefix() + "_" + name + "$" + DATABASE_NAME;
			//String db = name + "$" + DATABASE_NAME;
			st.execute("CREATE DATABASE " + db);
			st.execute("USE " + db);
			st.execute(
					"CREATE TABLE channels (sid INT PRIMARY KEY AUTO_INCREMENT," +
					"org VARCHAR(4) NOT NULL," +
					"site_no VARCHAR(32) UNIQUE NOT NULL," +
					"name VARCHAR(255), " + 
					"lon DOUBLE, lat DOUBLE, tz VARCHAR(12), active TINYINT(1))");
			st.execute(
					"CREATE TABLE data_types (type INT PRIMARY KEY," +
					"name VARCHAR(50))");
			st.execute(
					"CREATE TABLE channel_data_types (" +
					"sid INT NOT NULL auto_increment, " +
					"channel INT NOT NULL, type INT NOT NULL, " +
					"PRIMARY KEY (sid), " +
					"UNIQUE KEY channel (channel, type))");
			return true;
			
		}
		catch (SQLException e)
		{
			database.getLogger().log(Level.SEVERE, "SQLNWISDataSource.createDatabase() failed.", e);
		}
		return false;
	}

	public boolean databaseExists()
	{
		return defaultDatabaseExists(DATABASE_NAME);
	}

	public String getType()
	{
		return "nwis";
	}

	public void initialize(ConfigFile params)
	{
		String driver = params.getString("vdx.driver");
		String url = params.getString("vdx.url");
		String vdxPrefix = params.getString("vdx.vdxPrefix");
		
		name = params.getString("vdx.name");
		database = new VDXDatabase(driver, url, vdxPrefix);
	
	}

	public List<Station> getStations()
	{
		List<Station> result = new ArrayList<Station>();
		try
		{
			database.useDatabase(name + "$" + DATABASE_NAME);
			PreparedStatement ps = database.getPreparedStatement("SELECT sid, org, site_no, name, lon, lat, tz, active FROM channels");
			ResultSet rs = ps.executeQuery();
			while (rs.next())
			{
				int id = rs.getInt(1);
				String org = rs.getString(2);
				String siteNo = rs.getString(3);
				String name = rs.getString(4);
				double ln = rs.getDouble(5);
				double lt = rs.getDouble(6);
				String tz = rs.getString(7);
				boolean a = rs.getInt(8) == 0 ? false : true;
				Station s = new Station(id, org, siteNo, name, ln, lt, tz, a);
				result.add(s);
			}
			rs.close();
		}
		catch (Exception e)
		{
			database.getLogger().log(Level.SEVERE, "Could not get station list.");
			database.getLogger().log(Level.SEVERE, e.toString());
		}
		return result;
	}

	public Station getStation(String orgIn, String siteNoIn)
	{
		Station s = null;
		try
		{
			database.useDatabase(name + "$" + DATABASE_NAME);
			PreparedStatement ps = database.getPreparedStatement(
					"SELECT sid, org, site_no, name, lon, lat, tz, active FROM channels"
					+ " where org='" + orgIn + "' and site_no='" + siteNoIn + "';"
					);
			ResultSet rs = ps.executeQuery();
			rs.next();
			
			int id = rs.getInt(1);
			String org = rs.getString(2);
			String siteNo = rs.getString(3);
			String name = rs.getString(4);
			double ln = rs.getDouble(5);
			double lt = rs.getDouble(6);
			String tz = rs.getString(7);
			boolean a = rs.getInt(8) == 0 ? false : true;

			s = new Station(id, org, siteNo, name, ln, lt, tz, a);
			rs.close();
		}
		catch (Exception e)
		{
			database.getLogger().log(Level.SEVERE, "Could not get station list.");
			database.getLogger().log(Level.SEVERE, e.toString());
		}
		return s;
	}
	
	public List<DataType> getDataTypes()
	{
		List<DataType> result = new ArrayList<DataType>();
		try
		{
			database.useDatabase(name + "$" + DATABASE_NAME);
			PreparedStatement ps = database.getPreparedStatement("SELECT type, name FROM data_types");
			ResultSet rs = ps.executeQuery();
			while (rs.next())
			{
				int type = rs.getInt(1);
				String name = rs.getString(2);
				DataType dt = new DataType(type, name);
				result.add(dt);
			}
			rs.close();
		}
		catch (Exception e)
		{
			database.getLogger().log(Level.SEVERE, "Could not get data type list.");
		}
		return result;
	}
	
	public int getStationName(String code)
	{
		try
		{
			database.useDatabase(name + "$" + DATABASE_NAME);
			PreparedStatement ps = database.getPreparedStatement("SELECT sid FROM stations WHERE code=?");
			ps.setString(1, code);
			ResultSet rs = ps.executeQuery();
			rs.next();
			int id = rs.getInt(1);
			rs.close();
			return id;
		}
		catch (Exception e)
		{
			database.getLogger().log(Level.SEVERE, "Could not get station id.");
		}
		return -1;
	}

	public int getStationID(String code)
	{
		try
		{
			database.useDatabase(name + "$" + DATABASE_NAME);
			PreparedStatement ps = database.getPreparedStatement("SELECT sid FROM stations WHERE code=?");
			ps.setString(1, code);
			ResultSet rs = ps.executeQuery();
			rs.next();
			int id = rs.getInt(1);
			rs.close();
			return id;
		}
		catch (Exception e)
		{
			database.getLogger().log(Level.SEVERE, "Could not get station id.");
		}
		return -1;
	}
	


	public void insertDataType(DataType dt)
	{
		try
		{
			database.useDatabase(name + "$" + DATABASE_NAME);
			PreparedStatement ps = database.getPreparedStatement("INSERT IGNORE INTO data_types (type, name) VALUES (?,?)");
			ps.setInt(1, dt.getId());
			ps.setString(2, dt.getName());
			ps.execute();
		}
		catch (SQLException e)
		{
			database.getLogger().log(Level.SEVERE, "Could not insert data type.", e);
		}
	}
	
	public void insertRecord(Date d, Station station, DataType dt, double dd)
	{
		insertRecord(d, station, dt, dd, false);
	}
	
	public void insertRecord(Date d, Station station, DataType dt, double dd, boolean r)
	{
		
		System.out.print(".");
		
		String stationTable = station.getOrg()+station.getSiteNo();
		String dbName = name + "$" + DATABASE_NAME;
		
		if (! database.tableExists(dbName, stationTable))
			createStationTable(stationTable);
		
		try
		{
			database.useDatabase(dbName);
			String sql;
			if (r)
				sql = "REPLACE INTO ";
			else
				sql = "INSERT IGNORE INTO ";
			
			sql +=  stationTable + " (date, dataType, value) VALUES (?,?,?)";
			PreparedStatement ps = database.getPreparedStatement(sql);
			ps.setDouble(1, Util.dateToJ2K(d));
			ps.setInt(2, dt.getId());
			ps.setDouble(3, dd);
			ps.execute();
			
			Statement st = database.getStatement();
			st.execute("INSERT IGNORE INTO channel_data_types (sid, channel, type) " +
					" values (0, " + station.getId() + ", " + dt.getId() + ")");
		}
		catch (SQLException e)
		{
			database.getLogger().log(Level.SEVERE, "Could not insert record.", e);
		}
		
	}
	
	public int insertChannel(String code, String name, double lon, double lat)
	{
		int result = -1;
		
		try
		{
			database.useDatabase(name + "$" + DATABASE_NAME);
			PreparedStatement ps = database.getPreparedStatement("INSERT INTO channels (code, name, lon, lat) VALUES (?,?,?,?)");
			ps.setString(1, code);
			ps.setString(2, name);
			ps.setDouble(3, lon);
			ps.setDouble(4, lat);
			ps.execute();
		}
		catch (SQLException e)
		{
			database.getLogger().log(Level.SEVERE, "Could not insert channel.", e);
		}
		return result;
	}
	
	public boolean createStationTable(String stationTable)
	{
		boolean success = false;
		try
		{
			database.useDatabase(name + "$" + DATABASE_NAME);
			Statement st = database.getStatement();
			String sql = "CREATE TABLE " + stationTable + " (date double NOT NULL," +
						"dataType int NOT NULL," +
						"value double," +
						"PRIMARY KEY(date,dataType));";
			st.execute(sql);
			success = true;
		}
		catch (SQLException e)
		{
			database.getLogger().log(Level.SEVERE, "SQLNWISDataSource.createDatabase() failed.", e);
		}
		return success;
	}


	public List<String> getSelectors()
	{
		try
		{
			List<String> result = new ArrayList<String>();
			database.useDatabase(name + "$" + DATABASE_NAME);
			ResultSet rs = database.executeQuery("SELECT sid, org, site_no, name, lon, lat, tz FROM channels ORDER BY name");
			if (rs != null)
			{
				while (rs.next())
				{
					int sid = rs.getInt("sid");
					String org = rs.getString("org");
					String site_no = rs.getString("site_no");
					String name = rs.getString("name");
					double lon = rs.getDouble("lon");
					double lat = rs.getDouble("lat");
					String tz = rs.getString("tz");
					
					String r = String.format("%d:%s:%f:%s:%s:", sid, org, lon, name, site_no);
					
					StringBuffer types = new StringBuffer();
					Statement st = database.getConnection().createStatement();
					try
					{
						//ResultSet rs2 = st.executeQuery("SELECT DISTINCT dataType, name FROM " +  org + site_no + " inner join data_types ON dataType=data_types.type;");
						ResultSet rs2 = st.executeQuery("SELECT type, name FROM channel_data_types join data_types using (type) where channel = " + sid + ";");
//						ResultSet rs2 = st.executeQuery("SELECT channel_data_types.type, data_types.name " +
//								"FROM channel_data_types, data_types where channel_data_types.channel = " + sid +
//								"and channel_data_types.type = data_types.type order by data_types.type)");
						while (rs2.next())
						{
							types.append( rs2.getInt("channel_data_types.type") + "=" + rs2.getString("data_types.name") + "$");
						}
						rs2.close();
					}
					catch (Exception e)
					{}
					st.close();
					if (types.length()>0)
						result.add(r + types.toString().substring(0,types.length()-1));
				}
			}
			
			return result;
		}
		catch (SQLException e)
		{
			database.getLogger().log(Level.SEVERE, "SQLDataSource.defaultGetSelectors() failed.", e);
		}
		return null;
	}
	public RequestResult getData(Map<String, String> params)
	{
		
		System.out.println("getData params: " + params.toString());
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
			GenericDataMatrix data = getNWISData(cid, st, et, params.get("selectedTypes"));
			if (data != null)
				return new BinaryResult(data);
		}
		return null;
	}
	public GenericDataMatrix getNWISData(int cid, double st, double et, String selectedTypes)
	{
		GenericDataMatrix result = null;
		String sql;
		int numTypes=0;
		HashMap <Integer, Integer> types = new HashMap<Integer, Integer>();
		
		for (String type : selectedTypes.split(":"))
			types.put(Integer.parseInt(type), ++numTypes);
		
		try
		{
			database.useDatabase(name + "$" + DATABASE_NAME);
			PreparedStatement ps = database.getPreparedStatement("SELECT org, site_no FROM channels WHERE sid=?");
			ps.setInt(1, cid);
			ResultSet rs = ps.executeQuery();
			rs.next();
			String table = rs.getString(1) + rs.getString(2);
			rs.close();

			sql = "select date, dataType, value from " + table + " where (";
			for (String type : selectedTypes.split(":"))
				sql += " dataType=" + type + " or";
			sql = sql.substring(0, sql.length()-3);
			sql += ") AND date > ? and date < ? order by date, dataType";
			ps = database.getPreparedStatement(sql);			

			List<double[]> pts = new ArrayList<double[]>();
			ps.setDouble(1, st);
			ps.setDouble(2, et);
			rs = ps.executeQuery();
			System.out.println(ps.toString());
			double t=0;
			double[] d = null;
			while (rs.next())
			{
				if (rs.getDouble(1) != t)
				{
					if (d != null)
						pts.add(d);
					d = new double[numTypes + 1];
					t = rs.getDouble(1);
					d[0] = t;
				}
				
				int col = types.get(rs.getInt(2));
				d[col] = rs.getDouble(3); 
			}
			rs.close();

			if (pts.size() > 0)
				result = new GenericDataMatrix(pts);
		}
		catch (Exception e)
		{
			database.getLogger().log(Level.SEVERE, "SQLGenericDataSource.getGenericData()", e);
		}
		
//		if (result != null)
//			result.fillSparse();
		
		return result;
	}
}
