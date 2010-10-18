package gov.usgs.vdx.data.generic.variable;

import gov.usgs.util.ConfigFile;
import gov.usgs.util.Util;
import gov.usgs.util.UtilException;
import gov.usgs.vdx.client.VDXClient.DownsamplingType;
import gov.usgs.vdx.data.DataSource;
import gov.usgs.vdx.data.GenericDataMatrix;
import gov.usgs.vdx.data.SQLDataSource;
import gov.usgs.vdx.server.BinaryResult;
import gov.usgs.vdx.server.RequestResult;
import gov.usgs.vdx.server.TextResult;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;

/**
 * SQL Data Source for Generic Variable Data
 * 
 * @author Tom Parker, Loren Antolik
 */
public class SQLGenericVariableDataSource extends SQLDataSource implements DataSource {
	
	public static final String DATABASE_NAME	= "genericvariable";
	public static final boolean channels		= true;
	public static final boolean translations	= true;
	public static final boolean channelTypes	= false;
	public static final boolean ranks			= true;
	public static final boolean columns			= true;
	public static final boolean menuColumns		= false;

	/**
	 * Get database type, generic in this case
	 * @return type
	 */
	public String getType() 				{ return DATABASE_NAME; }	
	/**
	 * Get channels flag
	 * @return channels flag
	 */
	public boolean getChannelsFlag()		{ return channels; }
	/**
	 * Get translations flag
	 * @return translations flag
	 */
	public boolean getTranslationsFlag()	{ return translations; }
	/**
	 * Get channel types flag
	 * @return channel types flag
	 */
	public boolean getChannelTypesFlag()	{ return channelTypes; }
	/**
	 * Get ranks flag
	 * @return ranks flag
	 */
	public boolean getRanksFlag()			{ return ranks; }
	/**
	 * Get columns flag
	 * @return columns flag
	 */
	public boolean getColumnsFlag()			{ return columns; }
	/**
	 * Get menu columns flag
	 * @return menu columns flag
	 */
	public boolean getMenuColumnsFlag()		{ return menuColumns; }
	
	/**
	 * Initialize data source
	 * @param params config file
	 */
	public void initialize(ConfigFile params) {
		defaultInitialize(params);
		if (!databaseExists()) {
			createDatabase();
		}
	}
	
	/**
	 * De-Initialize data source
	 */
	public void disconnect() {
		defaultDisconnect();
	}

	/**
	 * Get flag if database exist
	 * @return true if database exists, false otherwise
	 */
	public boolean databaseExists() {
		return defaultDatabaseExists();
	}

	/**
	 * Create generic variable database
	 * @return true if successful, false otherwise
	 */
	public boolean createDatabase() {
		
		try {
			defaultCreateDatabase(channels, translations, channelTypes, ranks, columns, menuColumns);
			
			// create channel column xref table that is unique to the generic variable schema
			database.useDatabase(dbName);
			st	= database.getStatement();
			st.execute("CREATE TABLE channel_column_xref (ccid INT PRIMARY KEY AUTO_INCREMENT, cid INT, colid INT)");
			
			return true;

			// st.execute(
			// "CREATE TABLE channels (sid INT PRIMARY KEY AUTO_INCREMENT," +
			// "org VARCHAR(4) NOT NULL," +
			// "site_no VARCHAR(32) UNIQUE NOT NULL," +
			// "name VARCHAR(255), " + 
			// "lon DOUBLE, lat DOUBLE, tz VARCHAR(12), active TINYINT(1))");
			// st.execute(
			// "CREATE TABLE data_types (type INT PRIMARY KEY," +
			// "name VARCHAR(50))");
			// st.execute(
			// "CREATE TABLE channel_data_types (" +
			// "sid INT NOT NULL auto_increment, " +
			// "channel INT NOT NULL, type INT NOT NULL, " +
			// "PRIMARY KEY (sid), " +
			// "UNIQUE KEY channel (channel, type))");
			
		} catch (Exception e) {
			logger.log(Level.SEVERE, "SQLGenericVariableDataSource.createDatabase() failed.", e);
		}
		
		return false;
	}
	
	/**
	 * Create entry in the channels table and creates a table for that channel
	 * @param channelCode	channel code
	 * @param channelName	channel name
	 * @param lon			longitude
	 * @param lat			latitude
	 * @param height		height
	 * @return true if successful
	 */
	public boolean createChannel(String channelCode, String channelName, double lon, double lat, double height, int tid) {
		return defaultCreateChannel(channelCode, channelName, lon, lat, height, tid, channels, translations, ranks, columns);
	}
	
	/**
	 * Getter for data. 
	 * Search value of 'action' parameter and retrieve corresponding data.
	 * @param command to execute.
	 * @return request result
	 */
	public RequestResult getData(Map<String, String> params) {

		String action = params.get("action");
		
		if (action == null) {
			return null;
		
		} else if (action.equals("channels")) {
			return new TextResult(defaultGetChannels(channelTypes));
			
		} else if (action.equals("data")) {
			int cid					= Integer.parseInt(params.get("cid"));
			double st				= Double.parseDouble(params.get("st"));
			double et				= Double.parseDouble(params.get("et"));
			String selectedTypes	= params.get("selectedTypes");
			DownsamplingType ds = DownsamplingType.fromString(params.get("ds"));
			int dsInt		= Integer.parseInt(params.get("dsInt")); 
			GenericDataMatrix data = null;
			try{
				data = getGenericVariableData(cid, st, et, selectedTypes, getMaxRows(), ds, dsInt);
			} catch (UtilException e){
				return getErrorResult(e.getMessage());
			}
			if (data != null) {
				return new BinaryResult(data);
			}
		} else if (action.equals("supptypes")) {
			return getSuppTypes( true );
		
		} else if (action.equals("suppdata")) {
			return getSuppData( params, false );
		
		} else if (action.equals("metadata")) {
			return getMetaData( params, false );

		}
		return null;
	}
	
	/**
	 * Get Generic Variable data
	 * @param cid			channel id
	 * @param st			start time
	 * @param et			end time
	 * @param selectedTypes	???
	 * @return GenericDataMatrix
	 */
	public GenericDataMatrix getGenericVariableData(int cid, double st, double et, String selectedTypes, int maxrows, DownsamplingType ds, int dsInt) throws UtilException {		
		return defaultGetData(cid, 0, st, et, translations, ranks, maxrows, ds, dsInt);
		
		/*
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
			logger.info(ps.toString());
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
			logger.log(Level.SEVERE, "SQLGenericDataSource.getGenericData()", e);
		}
		
//		if (result != null)
//			result.fillSparse();
		
		return result;
		*/
	}

	/**
	 * Get one station by org and site number
	 * @param orgIn
	 * @param siteNoIn
	 */
	/*
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
			logger.log(Level.SEVERE, "Could not get station list.", e);
		}
		return s;
	}
	*/
	
	/**
	 * Get list of registered data types for this database
	 * @return List of DataTypes
	 */
	public List<DataType> getDataTypes() {
		List<DataType> result = new ArrayList<DataType>();
		try {
			database.useDatabase(dbName);
			ps = database.getPreparedStatement("SELECT type, name FROM data_types");
			rs = ps.executeQuery();
			while (rs.next()) {
				int type = rs.getInt(1);
				String name = rs.getString(2);
				DataType dt = new DataType(type, name);
				result.add(dt);
			}
			rs.close();
		} catch (Exception e) {
			logger.log(Level.SEVERE, "Could not get data type list.", e);
		}
		return result;
	}
	
	/**
	 * Get station id (the same as getStationID(String code))
	 * @param code station code
	 * @return station id
	 */
	public int getStationName(String code) {
		try {
			database.useDatabase(dbName);
			ps = database.getPreparedStatement("SELECT sid FROM stations WHERE code=?");
			ps.setString(1, code);
			rs = ps.executeQuery();
			rs.next();
			int id = rs.getInt(1);
			rs.close();
			return id;
		} catch (Exception e) {
			logger.log(Level.SEVERE, "Could not get station id.", e);
		}
		return -1;
	}

	/**
	 * Get station id (the same as getStationName(String code))
	 * @param code station code
	 * @return station id
	 */
	public int getStationID(String code) {
		try {
			database.useDatabase(dbName);
			ps = database.getPreparedStatement("SELECT sid FROM stations WHERE code=?");
			ps.setString(1, code);
			rs = ps.executeQuery();
			rs.next();
			int id = rs.getInt(1);
			rs.close();
			return id;
		} catch (Exception e) {
			logger.log(Level.SEVERE, "Could not get station id.", e);
		}
		return -1;
	}

	/**
	 * Create new data type in the database
	 * @param dt DataType to add
	 */
	public void insertDataType(DataType dt) {
		try {
			database.useDatabase(dbName);
			ps = database.getPreparedStatement("INSERT IGNORE INTO data_types (type, name) VALUES (?,?)");
			ps.setInt(1, dt.getId());
			ps.setString(2, dt.getName());
			ps.execute();
		} catch (Exception e) {
			logger.log(Level.SEVERE, "Could not insert data type.", e);
		}
	}
	
	/**
	 * insert new data record in the database
	 * @param d date 
	 * @param station station
	 * @param dt data type
	 * @param dd data value
	 */
	public void insertRecord(Date d, Station station, DataType dt, double dd) {
		insertRecord(d, station, dt, dd, false);
	}

	/**
	 * insert new data record in the database
	 * @param d date 
	 * @param station station
	 * @param dt data type
	 * @param dd data value
	 * @param is replace record?
	 */
	public void insertRecord(Date d, Station station, DataType dt, double dd, boolean r) {
		
		System.out.print(".");
		
		String stationTable = station.getOrg()+station.getSiteNo();
		
		if (! database.tableExists(dbName, stationTable))
			createStationTable(stationTable);
		
		try {
			database.useDatabase(dbName);
			String sql;
			if (r)
				sql = "REPLACE INTO ";
			else
				sql = "INSERT IGNORE INTO ";
			
			sql +=  stationTable + " (date, dataType, value) VALUES (?,?,?)";
			ps = database.getPreparedStatement(sql);
			ps.setDouble(1, Util.dateToJ2K(d));
			ps.setInt(2, dt.getId());
			ps.setDouble(3, dd);
			ps.execute();
			
			st = database.getStatement();
			st.execute("INSERT IGNORE INTO channel_data_types (sid, channel, type) " +
					" values (0, " + station.getId() + ", " + dt.getId() + ")");
		} catch (Exception e) {
			logger.log(Level.SEVERE, "Could not insert record.", e);
		}
		
	}
	
	/**
	 * Create table for station data
	 * @param stationTable table name
	 * @return true if successful, false otherwise
	 */
	public boolean createStationTable(String stationTable) {
		boolean success = false;
		try {
			database.useDatabase(dbName);
			st = database.getStatement();
			String sql = "CREATE TABLE " + stationTable + " (date double NOT NULL," +
						"dataType int NOT NULL," +
						"value double," +
						"PRIMARY KEY(date,dataType));";
			st.execute(sql);
			success = true;
		} catch (Exception e) {
			logger.log(Level.SEVERE, "SQLNWISDataSource.createDatabase() failed.", e);
		}
		return success;
	}

	/**
	 * Get channels list in format "sid:org:lon:name:site_no" from database
	 * @param db database name to query
	 */
	/*
	public List<String> getChannels()
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
			logger.log(Level.SEVERE, "SQLDataSource.defaultGetChannels() failed.", e);
		}
		return null;
	}
	*/
}
