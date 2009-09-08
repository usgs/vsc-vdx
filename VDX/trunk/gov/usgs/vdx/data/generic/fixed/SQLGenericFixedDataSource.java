package gov.usgs.vdx.data.generic.fixed;

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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;

import cern.colt.matrix.DoubleMatrix2D;

/**
 * @author Dan Cervelli
 */
public class SQLGenericFixedDataSource extends SQLDataSource implements DataSource {
	
	private static final String DATABASE_NAME = "generic";
	
	private List<GenericColumn> columns;
	private List<String> columnStrings;
	private Map<String, String> metadata;
	private String querySQL;
	private String name;
	
	/**
	 * Init object from given configuration file
	 */
	public void initialize(ConfigFile params) {		
		url = params.getString("vdx.url");
		if (url == null)
			throw new RuntimeException("config parameter vdx.url not found");
			
		driver = params.getString("vdx.driver");
		if (driver == null)
			throw new RuntimeException("config parameter vdx.driver not found");
		
		name = params.getString("vdx.name");
		if (name == null)
			throw new RuntimeException("config parameter vdx.name not found");
		
		setName(name);
		vdxPrefix = params.getString("vdx.vdxPrefix");
		if (vdxPrefix == null)
			throw new RuntimeException("config parameter vdx.vdxPrefix not found.");

		database = new VDXDatabase(driver, url, vdxPrefix);
		database.getLogger().info("vdx.name:" + name);
	}

	/**
	 * Getter for metadata
	 */
	private void getMetadata() {
		
		if (metadata != null) {
			return;
		}
		
		try {
			metadata = new HashMap<String, String>();
			database.useDatabase(name + "$" + DATABASE_NAME);
			ResultSet rs = database.getStatement().executeQuery("SELECT meta_key, meta_value FROM metadata");
			while (rs.next())
				metadata.put(rs.getString(1), rs.getString(2));
			rs.close();
			
		} catch (SQLException e) {
			database.getLogger().log(Level.SEVERE, "SQLGenericFixedDataSource.getMetadata()", e);
		}
	}
	
	/**
	 * Query database for described columns and construct
	 * sql to retrieve their data
	 */
	private void queryColumnData() {
		
		if (columns != null) {
			return;
		}
		
		try {
			columns			= new ArrayList<GenericColumn>();
			columnStrings	= new ArrayList<String>();
			Statement st	= database.getStatement();

			String db = name + "$" + DATABASE_NAME;
			if (!database.useDatabase(db))
				throw new RuntimeException("Can't connect to database");
			ResultSet rs = st.executeQuery("SELECT idx, name, description, unit, checked FROM cols ORDER BY idx ASC");
			while (rs.next()) {
				GenericColumn col	= new GenericColumn();
				col.index			= rs.getInt(1);
				col.name			= rs.getString(2);
				col.description		= rs.getString(3);
				col.unit			= rs.getString(4);
				col.checked			= rs.getInt(5) == 1;
				columns.add(col);
				columnStrings.add(col.toString());
			}
			rs.close();
			
			// include join to translations table
			// translations should correspond to the ordering of the columns in the cols table
			StringBuilder sb = new StringBuilder(256);
			sb.append("SELECT a.j2ksec as j2ksec, ");
			for (int i = 0; i < columns.size(); i++) {
				GenericColumn col = columns.get(i);
				sb.append("a." + col.name + " * b.c" + i + " + b.d" + i + " as " + col.name);
				if (i + 1 != columns.size()) {
					sb.append(", ");
				}
			}
			sb.append(" FROM [table] a JOIN translations b ON a.tid = b.tid WHERE a.j2ksec >= ? AND a.j2ksec <= ? ORDER BY a.j2ksec ASC");
			querySQL = sb.toString();
			
		} catch (SQLException e) {
			database.getLogger().log(Level.SEVERE, "SQLGenericFixedDataSource.queryColumnData()", e);
		}
	}
	
	/**
	 * Create generic database
	 */
	public boolean createDatabase() {
		
		try {
			name = getName();
			String db = name + "$" + DATABASE_NAME;
			if (!createDefaultDatabase(db, 0, true, true)) {
				return false;
			}
			
			Statement st = database.getStatement();
			database.useDatabase(db);
			st.execute(
					"CREATE TABLE cols (idx INT PRIMARY KEY AUTO_INCREMENT," +
					"name VARCHAR(255) UNIQUE," +
					"description VARCHAR(255)," + 
					"unit VARCHAR(255)," +
					"checked TINYINT)");
			st.execute(
					"CREATE TABLE metadata (mid INT PRIMARY KEY AUTO_INCREMENT," +
					"meta_key VARCHAR(255)," +
					"meta_value VARCHAR(255))");
			return true;
			
		} catch (SQLException e) {
			database.getLogger().log(Level.SEVERE, "SQLGenericFixedDataSource.createDatabase() failed.", e);
		}
		return false;
	}
	
	public boolean createTranslationTable() {
		
		// default return value
		boolean returnValue = false;
		String sql = "";
		queryColumnData();
		
		try {
			for (int i = 0; i < columns.size(); i++) {
				sql += "c" + i + " DOUBLE DEFAULT 1, d" + i + " DOUBLE DEFAULT 0";
				if (i != columns.size() - 1)
					sql += ",";
			}			
			database.useDatabase(name + "$" + DATABASE_NAME);
			Statement st = database.getStatement();
			st.execute("CREATE TABLE translations (tid INT PRIMARY KEY AUTO_INCREMENT,code VARCHAR(16)," + sql + ")");
			st.execute("INSERT INTO translations (tid, code) VALUES (1, 'default')");
			
		} catch (SQLException e) {
			database.getLogger().log(Level.SEVERE, "SQLGenericFixedDataSource.createTranslationTable() failed", e);
		}
		
		return returnValue;
	}

	/**
	 * Create channel
	 * @param channel channel code
	 * @param channelName
	 * @param lon
	 * @param lat
	 * @return flag if operation successful
	 */
	public boolean createChannel(String channel, String channelName, double lon, double lat) {
		
		if (!defaultChannelExists(DATABASE_NAME, channel)) {
			queryColumnData();
			String[] cols = new String[columns.size()];
			for (int i = 0; i < cols.length; i++) {
				cols[i] = columns.get(i).name;
			}		
			return createDefaultChannel(name + "$" + DATABASE_NAME, cols.length, channel, channelName, lon, lat, cols, true, true);
		} else {
			return true;
		}
	}
	
	/**
	 * createColumn
	 * @param gc
	 */
	public boolean createColumn (GenericColumn gc) {
		
		try {			
			database.useDatabase(name + "$" + DATABASE_NAME);
			PreparedStatement ps = database.getPreparedStatement(
					"INSERT IGNORE INTO cols (name, description, unit, checked) " +
					"VALUES (?,?,?,?)");
			ps.setString(1, gc.name);
			ps.setString(2, gc.description);
			ps.setString(3, gc.unit);
			ps.setBoolean(4, gc.checked);
			ps.execute();
			return true;
			
		} catch (SQLException e) {
			database.getLogger().log(Level.SEVERE, "SQLGenericFixedDataSource.createColumn() failed.", e);
		}
		return false;
	}
	
	/**
	 * Get flag if database exist
	 */
	public boolean databaseExists() {
		return defaultDatabaseExists(name + "$" + DATABASE_NAME);
	}

	/**
	 * Get database type, generic in this case
	 */
	public String getType() {
		return "generic";
	}

	/**
	 * Get channels list in format "sid:code:name:lon:lat" from database
	 * @param db database name to query
	 */
	public List<String> getSelectors() {
		return defaultGetSelectors(DATABASE_NAME);
	}

	/**
	 * Get selector name
	 * @param plural if we need selector name in the plural form
	 */
	public String getSelectorName(boolean plural) {
		return plural ? "Stations" : "Station";
	}

	/**
	 * Getter for selector string
	 */
	public String getSelectorString() {
		String ss = metadata.get("selectorString");
		if (ss == null)
			return "Channels";
		else
			return ss;
	}
	
	/**
	 * Getter for data source description
	 */
	public String getDescription() {
		String d = metadata.get("description");
		if (d == null)
			return "no description";
		else
			return d;
	}
	
	/**
	 * Getter for data source title
	 */
	public String getTitle() {
		String t = metadata.get("title");
		if (t == null)
			return "Generic Data";
		else
			return t;
	}
	
	/**
	 * Getter for data source time shortcuts
	 */
	public String getTimeShortcuts() {
		String ts = metadata.get("timeShortcuts");
		if (ts == null)
			return "-6h,-24h,-3d,-1w,-1m,-1y";
		else
			return ts;
	}
	
	/**
	 * Getter for data. Search value of 'action' parameter and retrieve corresponding data.
	 * @param command to execute.
	 * 
	 */
	public RequestResult getData(Map<String, String> params) {
		String action = params.get("action");
		if (action == null)
			return null;

		queryColumnData();
		getMetadata();
		
		if (action.equals("genericMenu")) {
			List<String> result = new ArrayList<String>(columnStrings.size() + 5);
			result.add(getTitle());
			result.add(getDescription());
			result.add(getSelectorString());
			result.add(getTimeShortcuts());
			result.add(Integer.toString(columnStrings.size()));
			for (String s : columnStrings) {
				result.add(s);
			}
			return new TextResult(result);
		} else if (action.equals("selectors")) {
			List<String> s = getSelectors();
			return new TextResult(s);
		} else if (action.equals("data")) {
			int cid = Integer.parseInt(params.get("cid"));
			double st = Double.parseDouble(params.get("st"));
			double et = Double.parseDouble(params.get("et"));
			GenericDataMatrix data = getGenericData(cid, st, et);
			if (data != null)
				return new BinaryResult(data);
		}
		return null;
	}

	/**
	 * Get data for one channel
	 * @param cid channel code
	 * @param st start time
	 * @param et end time
	 * @return
	 */
	public GenericDataMatrix getGenericData(int cid, double st, double et) {
		
		GenericDataMatrix result = null;
		
		try {
			database.useDatabase(name + "$" + DATABASE_NAME);
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
			while (rs.next()) {
				double[] d = new double[columns.size() + 1];
				for (int i = 0; i < columns.size() + 1; i++)
					d[i] = rs.getDouble(i + 1);
				pts.add(d);
			}
			rs.close();
			
			if (pts.size() > 0)
				result = new GenericDataMatrix(pts);
			
		} catch (Exception e) {
			database.getLogger().log(Level.SEVERE, "SQLGenericFixedDataSource.getGenericData()", e);
		}
		return result;
	}
	
	/**
	 * Gets translation id from database using the parameters passed.  Used to determine if the 
	 * translation exists in the database for inserting a potentially new translation.
	 * 
	 * @param code		station code
	 * @param gdm		generic data matrix containing the translations
	 * 
	 * @return tid		translation id of the translation.  -1 if not found.
	 */
	public int getTranslation (String code, GenericDataMatrix gdm) {
		
		// default the tid as a return value
		int tid = -1;
		PreparedStatement ps;
		ResultSet rs;
		String sql = "";
		
		// try looking up the translation in the database
		try {
			
			DoubleMatrix2D dm = gdm.getData();
			
			// iterate through the generic data matrix to get a list of the columns and their values
			for (int i = 0; i < dm.rows(); i++) {
				sql += "AND c" + i + " = " + dm.get(i, 0) + " AND d" + i + " = " + dm.get(i, 1) + " ";
			}
			
			// build and execute the query
			database.useDatabase(name + "$" + DATABASE_NAME);
			ps = database.getPreparedStatement("SELECT tid FROM translations WHERE code=? " + sql);
			ps.setString(1, code);
			rs = ps.executeQuery();
			if (rs.next()) {
				tid = rs.getInt(1);
			}
			rs.close();
			
		// catch SQLException
		} catch (SQLException e) {
			database.getLogger().log(Level.SEVERE, "SQLGenericFixedDataSource.getTranslation() failed.", e);
		}
		
		// return the translation id
		return tid;
	}
	
	/**
	 * Inserts a translation in the translations table, and assigns the channels table to use this translation as its default
	 * 
	 * @param code		station code
	 * @param gdm		generic data matrix containing the translations
	 * 
	 * @return tid		translation id of this translation.  -1 if not found
	 */
	public int createTranslation(String code, GenericDataMatrix gdm) {
		
		// default local variables
		int tid = -1;
		PreparedStatement ps;
		String columns	= "";
		String values	= "";
		
		// try to create the translation, if it doesn't exist
		try {
			
			// lookup the translation to see if it exists in the database yet
			tid = getTranslation(code, gdm);
			
			// use the correct database
			if (tid == -1) {
				
				DoubleMatrix2D dm = gdm.getData();
				
				// iterate through the generic data matrix to get a list of the values
				for (int i = 0; i < dm.rows(); i++) {
					columns += "c" + i + ",d" + i + ",";
					values	+= dm.get(i, 0) + "," + dm.get(i, 1) + ",";
				}
				columns += "code";
				values  += "'" + code + "'";
				
				// insert the translation into the database
				ps = database.getPreparedStatement("INSERT INTO translations (" + columns + ") VALUES (" + values + ")");
				ps.execute();
				tid = getTranslation(code, gdm);
			}
			
			// update the channels table with the current tid
			ps = database.getPreparedStatement("UPDATE channels SET tid = ? WHERE code = ?");
			ps.setInt(1, tid);
			ps.setString(2, code);
			ps.execute();
			
		// catch SQLException
		} catch (SQLException e) {
			database.getLogger().log(Level.SEVERE, "SQLGenericFixedDataSource.insertTranslation() failed.", e);
		}
		
		// return the translation id
		return tid;		
	}
	
	/**
	 * Insert data 
	 * @param table table name to insert
	 * @param d 2d matrix of data
	 */
	public void insertData(String table, GenericDataMatrix gdm) {
		
		String[] colNames = gdm.getColumnNames();
		DoubleMatrix2D data = gdm.getData();
		int tid = -1;

		database.useDatabase(name + "$" + DATABASE_NAME);
		
		// get the current tid from the channels table
		try {
			PreparedStatement ps = database.getPreparedStatement("SELECT tid FROM channels where code=?");
			ps.setString(1, table);
			ResultSet rs = ps.executeQuery();
			rs.next();
			tid = rs.getInt(1);
			rs.close();
		} catch (Exception e) {
			database.getLogger().log(Level.SEVERE, "SQLGenericFixedDataSource.insertData()", e);
		}
		
		Statement st = database.getStatement();

		for (int i = 0; i < gdm.rows(); i++) {
			StringBuffer columns	= new StringBuffer();
			StringBuffer values		= new StringBuffer();
			
			// append the columns which DO appear in the column list
			for (int j = 0; j < colNames.length; j++) {
				columns.append(colNames[j] + ",");
				values.append(data.getQuick(i, j) + ",");
			}
			
			// append the tid
			columns.append("tid");
			values.append(tid);
			
			StringBuffer sql = new StringBuffer();
			sql.append("INSERT IGNORE INTO " + table + " (" + columns + ") VALUES (" + values + ")");
			
			try {
				st.execute(sql.toString());
			} catch (SQLException e) {
				database.getLogger().log(Level.SEVERE, "SQLGenericFixedDataSource.insertData() failed.", e);
			}
		}
	}
	
	/**
	 * Getter for column list
	 */
	public List<GenericColumn> getColumns() {
		return columns;
	}
	
	/**
	 * Inserts data into VALVE2 Strain Database.  Assumes the translation and offsets are already set properly in the database.
	 * 
	 * @param code	station code
	 * @param t		j2ksec
	 * @param s1	strain1 value
	 * @param s2	strain2 value
	 * @param g		ground voltage
	 * @param bar	barometer
	 * @param h		hole temperature
	 * @param i		instrument voltage
	 * @param r		rainfall
	 */
	public void insertV2StrainData(String code, double t, double s1, double s2, double g, double bar, double h, double i, double r) {
		
		try {
			
			// default some variables
			int tid = -1;
			int eid = -1;
            
            // lower case the code because that's how the table names are in the database
            code.toLowerCase();
			
			// set the database
			database.useV2Database("strain");
			
			// get the translation and offset
            PreparedStatement ps = database.getPreparedStatement(
            		"SELECT curTrans, curEnvTrans FROM stations WHERE code=?");
            ps.setString(1, code);
            ResultSet rs = ps.executeQuery();
            if (rs.next()) {
            	tid = rs.getInt(1);
            	eid = rs.getInt(2);
            }
            rs.close();

            // create the strain entry
            ps = database.getPreparedStatement("INSERT IGNORE INTO " + code + "strain VALUES (?,?,?,?,?,?)");
			ps.setDouble(1, t);
			ps.setString(2, Util.j2KToDateString(t));
			ps.setDouble(3, s1);
			ps.setDouble(4, s2);
			ps.setDouble(5, g);
			ps.setDouble(6, tid);
			ps.execute();
			
			// create the environment entry
            ps = database.getPreparedStatement("INSERT IGNORE INTO " + code + "env VALUES (?,?,?,?,?,?,?,?)");
			ps.setDouble(1, t);
			ps.setString(2, Util.j2KToDateString(t));
			ps.setDouble(3, bar);
			ps.setDouble(4, h);
			ps.setDouble(5, i);
			ps.setDouble(6, r);
			ps.setDouble(7, g);
			ps.setDouble(8, eid);
			ps.execute();
			
		} catch (SQLException e) {
			database.getLogger().log(Level.SEVERE, "SQLGenericFixedDataSource.insertV2StrainData() failed.", e);
		}		
	}
	
	public void insertV2GasData(int sid, double t, double co2) {
		
		try {
			
			// set the database
			database.useV2Database("gas");

            // create the tilt entry
			PreparedStatement ps = database.getPreparedStatement("INSERT IGNORE INTO co2 VALUES (?,?,?,?)");
			ps.setDouble(1, t);
			ps.setInt(2, sid);
			ps.setString(3, Util.j2KToDateString(t));
			ps.setDouble(4, co2);
			ps.execute();
			
		} catch (SQLException e) {
			database.getLogger().log(Level.SEVERE, "SQLGenericFixedDataSource.insertV2GasData() failed.", e);
		}		
	}
}
