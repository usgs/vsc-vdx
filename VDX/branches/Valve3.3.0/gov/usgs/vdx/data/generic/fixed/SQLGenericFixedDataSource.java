package gov.usgs.vdx.data.generic.fixed;

import gov.usgs.util.ConfigFile;
import gov.usgs.util.Util;
import gov.usgs.vdx.data.DataSource;
import gov.usgs.vdx.data.Column;
import gov.usgs.vdx.data.GenericDataMatrix;
import gov.usgs.vdx.data.SQLDataSource;
import gov.usgs.vdx.server.BinaryResult;
import gov.usgs.vdx.server.RequestResult;
import gov.usgs.vdx.server.TextResult;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;

/**
 * SQL Data Source for Generic Fixed Data
 * 
 * @author Dan Cervelli, Loren Antolik
 */
public class SQLGenericFixedDataSource extends SQLDataSource implements DataSource {
	
	public final String DATABASE_NAME	= "genericfixed";
	public final boolean channels		= true;
	public final boolean translations	= true;
	public final boolean channelTypes	= false;
	public final boolean ranks			= true;
	public final boolean columns		= true;
	public final boolean plotColumns	= false;

	/**
	 * Get database type, generic in this case
	 * return type
	 */
	public String getType() 				{ return DATABASE_NAME; }	
	public boolean getChannelsFlag()		{ return channels; }
	public boolean getTranslationsFlag()	{ return translations; }
	public boolean getChannelTypesFlag()	{ return channelTypes; }
	public boolean getRanksFlag()			{ return ranks; }
	public boolean getColumnsFlag()			{ return columns; }
	public boolean getPlotColumnsFlag()		{ return plotColumns; }
	
	/**
	 * Initialize data source
	 */
	public void initialize(ConfigFile params) {
		defaultInitialize(params);
		if (!databaseExists()) {
			createDatabase();
		}
	}

	/**
	 * Get flag if database exist
	 * @return true if successful
	 */
	public boolean databaseExists() {
		return defaultDatabaseExists();
	}
	
	/**
	 * Create generic fixed database
	 * @return true if successful
	 */
	public boolean createDatabase() {
		
		try {
			defaultCreateDatabase(channels, translations, channelTypes, ranks, columns, plotColumns);
			
			// create metadata table that is unique to the generic fixed schema
			database.useDatabase(dbName);			
			st = database.getStatement();
			st.execute(
					"CREATE TABLE metadata (mid INT PRIMARY KEY AUTO_INCREMENT," +
					"meta_key VARCHAR(255)," +
					"meta_value VARCHAR(255))");
			
			return true;
			
		} catch (Exception e) {
			logger.log(Level.SEVERE, "SQLGenericFixedDataSource.createDatabase() failed.", e);
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
	public boolean createChannel(String channelCode, String channelName, double lon, double lat, double height) {
		return defaultCreateChannel(channelCode, channelName, lon, lat, height, channels, translations, ranks, columns);
	}
	
	
	public boolean createTranslation() {
		return defaultCreateTranslation();
	}
	
	/**
	 * Getter for data. 
	 * Search value of 'action' parameter and retrieve corresponding data.
	 * @param command to execute. 
	 */
	public RequestResult getData(Map<String, String> params) {
		
		String action = params.get("action");
		
		if (action == null) {
			return null;
		
		} else if (action.equals("channels")) {
			return new TextResult(defaultGetChannels(channelTypes));
			
		} else if (action.equals("columns")) {
			return new TextResult(defaultGetMenuColumns(plotColumns));
			
		} else if (action.equals("ranks")) {
			return new TextResult(defaultGetRanks());
			
		} else if (action.equals("data")) {
			int cid		= Integer.parseInt(params.get("ch"));
			int rid		= Integer.parseInt(params.get("rk"));
			double st	= Double.parseDouble(params.get("st"));
			double et	= Double.parseDouble(params.get("et"));
			GenericDataMatrix data = getGenericFixedData(cid, rid, st, et);
			if (data != null) {
				return new BinaryResult(data);
			}
			
		} else if (action.equals("genericMenu")) {
			return new TextResult(getGenericMenu());
		}
		return null;
	}
	
	private List<String> getGenericMenu() {
		List<String> genericMenuString = new ArrayList<String>();
		return genericMenuString;
	}

	/**
	 * Get Generic Fixed data
	 * @param cid	channel id
	 * @param rid	rank id
	 * @param st	start time
	 * @param et	end time
	 * @return GenericDataMatrix
	 */
	public GenericDataMatrix getGenericFixedData(int cid, int rid, double st, double et) {
		return defaultGetData(cid, rid, st, et, translations, ranks);
	}
	

	/**
	 * Getter for metadata
	 */
	public Map<String, String> getMetadata() {

		Map<String, String> metadata = new HashMap<String, String>();
		
		try {
			database.useDatabase(dbName);
			ps	= database.getPreparedStatement("SELECT meta_key, meta_value FROM metadata");
			rs	= ps.executeQuery();
			while (rs.next()) {
				metadata.put(rs.getString(1), rs.getString(2));
			}
			rs.close();
			
			return metadata;
			
		} catch (Exception e) {
			logger.log(Level.SEVERE, "SQLGenericFixedDataSource.getMetadata()", e);
		}
		
		return metadata;
	}
	
	/**
	 * Getter for selector string
	 */
	public String getChannelString(Map<String, String> metadata) {
		String ss = metadata.get("channelString");
		if (ss == null)
			return "Channels";
		else
			return ss;
	}
	
	/**
	 * Getter for data source description
	 */
	public String getDescription(Map<String, String> metadata) {
		String d = metadata.get("description");
		if (d == null)
			return "no description";
		else
			return d;
	}
	
	/**
	 * Getter for data source title
	 */
	public String getTitle(Map<String, String> metadata) {
		String t = metadata.get("title");
		if (t == null)
			return "Generic Data";
		else
			return t;
	}
	
	/**
	 * Getter for data source time shortcuts
	 */
	public String getTimeShortcuts(Map<String, String> metadata) {
		String ts = metadata.get("timeShortcuts");
		if (ts == null)
			return "-6h,-24h,-3d,-1w,-1m,-1y";
		else
			return ts;
	}
	
	/**
	 * Insert data into the database using the parameters passed
	 * @param channelCode
	 * @param gdm
	 */
	public void insertData (String channelCode, GenericDataMatrix gdm, int rid) {
		defaultInsertData(channelCode, gdm, translations, ranks, rid);
	}
	
	public int insertTranslation (String channelCode, GenericDataMatrix gdm) {
		return defaultInsertTranslation(channelCode, gdm);
	}
	
	public boolean insertColumn(Column column) {
		return defaultInsertColumn(column);
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
            ps = database.getPreparedStatement(
            		"SELECT curTrans, curEnvTrans FROM stations WHERE code=?");
            ps.setString(1, code);
            rs = ps.executeQuery();
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
			
		} catch (Exception e) {
			logger.log(Level.SEVERE, "SQLGenericFixedDataSource.insertV2StrainData() failed.", e);
		}		
	}
	
	public void insertV2GasData(int sid, double t, double co2) {
		
		try {
			
			// set the database
			database.useV2Database("gas");

            // create the tilt entry
			ps = database.getPreparedStatement("INSERT IGNORE INTO co2 VALUES (?,?,?,?)");
			ps.setDouble(1, t);
			ps.setInt(2, sid);
			ps.setString(3, Util.j2KToDateString(t));
			ps.setDouble(4, co2);
			ps.execute();
			
		} catch (Exception e) {
			logger.log(Level.SEVERE, "SQLGenericFixedDataSource.insertV2GasData() failed.", e);
		}		
	}
}
