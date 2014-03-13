package gov.usgs.vdx.data.generic.fixed;

import gov.usgs.plot.data.GenericDataMatrix;
import gov.usgs.util.ConfigFile;
import gov.usgs.util.UtilException;
import gov.usgs.vdx.client.VDXClient.DownsamplingType;
import gov.usgs.vdx.data.DataSource;
import gov.usgs.vdx.data.SQLDataSource;
import gov.usgs.vdx.server.BinaryResult;
import gov.usgs.vdx.server.RequestResult;
import gov.usgs.vdx.server.TextResult;

import java.util.ArrayList;
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
	public final boolean menuColumns	= false;

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
			defaultCreateDatabase(channels, translations, channelTypes, ranks, columns, menuColumns);			
			logger.log(Level.INFO, "SQLGenericFixedDataSource.createDatabase(" + database.getDatabasePrefix() + "_" + dbName + ") succeeded.");
			return true;
			
			/*
			// create metadata table that is unique to the generic fixed schema
			database.useDatabase(dbName);			
			st = database.getStatement();
			st.execute(
					"CREATE TABLE metadata (mid INT PRIMARY KEY AUTO_INCREMENT," +
					"meta_key VARCHAR(255)," +
					"meta_value VARCHAR(255))");
			*/
			
		} catch (Exception e) {
			logger.log(Level.SEVERE, "SQLGenericFixedDataSource.createDatabase(" + database.getDatabasePrefix() + "_" + dbName + ") failed.", e);
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
	
	
	public boolean createTranslation() {
		return defaultCreateTranslation();
	}
	
	/**
	 * Getter for data. 
	 * Search value of 'action' parameter and retrieve corresponding data.
	 * @param params command to execute. 
	 * @return request result
	 */
	public RequestResult getData(Map<String, String> params) {
		
		String action = params.get("action");
		
		if (action == null) {
			return null;
		
		} else if (action.equals("channels")) {
			return new TextResult(defaultGetChannels(channelTypes));
			
		} else if (action.equals("columns")) {
			return new TextResult(defaultGetMenuColumns(menuColumns));
			
		} else if (action.equals("ranks")) {
			return new TextResult(defaultGetRanks());
			
		} else if (action.equals("data")) {
			int cid		= Integer.parseInt(params.get("ch"));
			int rid		= Integer.parseInt(params.get("rk"));
			double st	= Double.parseDouble(params.get("st"));
			double et	= Double.parseDouble(params.get("et"));
			DownsamplingType ds = DownsamplingType.fromString(params.get("ds"));
			int dsInt		= Integer.parseInt(params.get("dsInt")); 
			GenericDataMatrix data = null;
			try {
				data = getGenericFixedData(cid, rid, st, et, getMaxRows(),  ds, dsInt);
			} catch (UtilException e){
				return getErrorResult(e.getMessage());
			}
			if (data != null) {
				return new BinaryResult(data);
			}
			
		} else if (action.equals("genericMenu")) {
			return new TextResult(getGenericMenu());
			
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
	 * Yield empty list of strings
	 * @return empty List of strings
	 */
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
	public GenericDataMatrix getGenericFixedData(int cid, int rid, double st, double et, int maxrows, DownsamplingType ds, int dsInt) throws UtilException {
		return defaultGetData(cid, rid, st, et, translations, ranks, maxrows,  ds, dsInt);
	}
	

	/**
	 * Getter for metadata
	 * @return Map of metadata (name -> value)
	 *
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
	}*/
	
	/**
	 * Getter for selector string
	 * @param metadata Mapping from names to values
	 * @return value for "channelString", "Channels" if missing
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
	 * @param metadata Mapping from names to values
	 * @return value for "description", "no description" if missing
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
	 * @param metadata Mapping from names to values
	 * @return value for "title", "Generic Data" if missing
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
	 * @param metadata Mapping from names to values
	 * @return value for "timeShortcuts", "-6h,-24h,-3d,-1w,-1m,-1y" if missing
	 */
	public String getTimeShortcuts(Map<String, String> metadata) {
		String ts = metadata.get("timeShortcuts");
		if (ts == null)
			return "-6h,-24h,-3d,-1w,-1m,-1y";
		else
			return ts;
	}
}
