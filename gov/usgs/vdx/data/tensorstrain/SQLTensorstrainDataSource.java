package gov.usgs.vdx.data.tensorstrain;

import gov.usgs.math.DownsamplingType;
import gov.usgs.util.ConfigFile;
import gov.usgs.util.UtilException;
import gov.usgs.vdx.data.Channel;
import gov.usgs.vdx.data.Column;
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
 * SQL Data Source for Tensorstrain Data
 *
 * @author Max Kokoulin
 */
public class SQLTensorstrainDataSource extends SQLDataSource implements DataSource {
	
	public static final String DATABASE_NAME	= "tensorstrain";
	public static final boolean channels		= true;
	public static final boolean translations	= true;
	public static final boolean channelTypes	= false;
	public static final boolean ranks			= true;
	public static final boolean columns			= true;
	public static final boolean menuColumns		= true;
	
	public static final Column[] DATA_COLUMNS	= new Column[] {
		new Column(1, "CH0",		"CH0",					"nanostrain",			false, true, false),
		new Column(2, "CH1",		"CH1",					"nanostrain",			false, true, false), 
		new Column(3, "CH2",		"CH2",					"nanostrain",			false, true, false),
		new Column(4, "CH3",		"CH3",					"nanostrain",			false, true, false), 
		new Column(5, "eEEpeNN",	"eEE+eNN",				"nanostrain",			false, true, false), 
		new Column(6, "eEEmeNN",	"eEE-eNN",				"nanostrain",			false, true, false),
		new Column(7, "e2EN",		"e2EN)",				"nanostrain",			false, true, false), 
		new Column(8, "baro",		"barometer",			"hPa",					false, true, false), 
		new Column(9, "rain",		"rainfall",				"mm",					false, true, false),
		new Column(10,"pore",		"pore pressure",		"hPa",					false, true, false)};
		
	
	public static final Column[] MENU_COLUMNS	= new Column[] {
		new Column(1, "CH0",		"CH0",					"nanostrain",			false, true, false),
		new Column(2, "CH1",		"CH1",					"nanostrain",			false, true, false), 
		new Column(3, "CH2",		"CH2",					"nanostrain",			false, true, false),
		new Column(4, "CH3",		"CH3",					"nanostrain",			false, true, false), 
		new Column(5, "eEEpeNN",	"eEE+eNN",				"nanostrain",			false, true, false), 
		new Column(6, "eEEmeNN",	"eEE-eNN",				"nanostrain",			false, true, false),
		new Column(7, "e2EN",		"e2EN",					"nanostrain",			false, true, false), 
		new Column(8, "eXXmeYY",	"eXX-eYY",				"nanostrain",			false, true, false),
		new Column(9, "e2XY",		"e2XY",					"nanostrain",			false, true, false), 
		new Column(10,"baro",		"barometer",			"hPa",					false, true, false), 
		new Column(11,"rain",		"rainfall",				"mm",					false, true, false),
		new Column(12,"pore",		"pore pressure",		"hPa",					false, true, false)};

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
	public boolean getMenuColumnsFlag()		{ return menuColumns; }
	
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
	 * De-Initialize data source
	 */
	public void disconnect() {
		defaultDisconnect();
	}

	/**
	 * Get flag if database exists
	 */
	public boolean databaseExists() {
		return defaultDatabaseExists();
	}
	
	/**
	 * Create tensorstrain database
	 */
	public boolean createDatabase() {
		
		try {
			defaultCreateDatabase(channels, translations, channelTypes, ranks, columns, menuColumns);
			
			// columns table
			for (int i = 0; i < DATA_COLUMNS.length; i++) {
				defaultInsertColumn(DATA_COLUMNS[i]);
			}
			
			// menu columns table
			for (int i = 0; i < MENU_COLUMNS.length; i++) {
				defaultInsertMenuColumn(MENU_COLUMNS[i]);
			}
			
			// translations table
			defaultCreateTranslation();
			
			// alter the channels table to add an azimuth column. 
			database.useDatabase(dbName);
			st = database.getStatement();
			st.execute("ALTER TABLE channels ADD natural_azimuth DOUBLE DEFAULT 0");
			return true;
		} catch (Exception e) {
			logger.log(Level.SEVERE, "SQLTensorstrainDataSource.createDatabase() failed.", e);
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
	 * @param tid           translation id
	 * @param natural_azimuth		azimuth of the deformation source
	 * @return true if successful
	 */	
	public boolean createChannel(String channelCode, String channelName, double lon, double lat, double height, int tid, double natural_azimuth) {
		Channel channel = new Channel(0, channelCode, channelName, lon, lat, natural_azimuth, height);
		return defaultCreateTiltChannel(channel, tid, natural_azimuth, channels, translations, ranks, columns);
	}

	/**
	 * Getter for data. 
	 * Search value of 'action' parameter and retrieve corresponding data.
	 * @param params command to execute, map of parameter-value pairs.
	 * @return requested result
	 */	
	public RequestResult getData(Map<String, String> params) {
		
		String action = params.get("action");
		
		if (action == null) {
			return null;
		
		} else if (action.equals("channels")) {
			return new TextResult(defaultGetChannels(channelTypes));			

		} else if (action.equals("ranks")) {
			return new TextResult(defaultGetRanks());			

		} else if (action.equals("columns")) {
			return new TextResult(defaultGetMenuColumns(menuColumns));
			
		} else if (action.equals("azimuths")) {
			return new TextResult(getAzimuths());
			
		} else if (action.equals("data")) {
			int cid			= Integer.parseInt(params.get("ch"));
			int rid			= Integer.parseInt(params.get("rk"));
			double st		= Double.parseDouble(params.get("st"));
			double et		= Double.parseDouble(params.get("et"));
			DownsamplingType ds = DownsamplingType.fromString(params.get("ds"));
			int dsInt		= Integer.parseInt(params.get("dsInt")); 
			return getTensorstrainData(cid, rid, st, et, getMaxRows(), ds, dsInt);
			
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
	 * Get Tensorstrain Station data
	 * @param cid	   channel id
	 * @param rid	   rank id
	 * @param st	   start time
	 * @param et	   end time
	 * @param maxrows  maximum nbr of rows returned
	 * @param ds       type of downsampling
	 * @param dsInt    downsampling argument
	 * @return requested result
	 */	
	public RequestResult getTensorstrainData(int cid, int rid, double st, double et, int maxrows, DownsamplingType ds, int dsInt) {
		
		double[] dataRow;		
		List<double[]> pts	= new ArrayList<double[]>();
		int columnsReturned	= 0;
		double value;
		
		try {
			
			database.useDatabase(dbName);
			
			// look up the channel code from the channels table, which is the name of the table to query
			Channel channel	= defaultGetChannel(cid, channelTypes);
			columnsReturned	= 12;
			
			// calculate the num of rows to limit the query to
			int tempmaxrows;
			if (rid != 0) {
				tempmaxrows = maxrows;
			} else {
				tempmaxrows = maxrows * defaultGetNumberOfRanks();
			}

			// build the sql
			sql  = "SELECT j2ksec, c.rid, ";
			sql += "       CH0 * cCH0 + dCH0, ";
			sql += "       CH1 * cCH1 + dCH1, ";
			sql += "       CH2 * cCH2 + dCH2, ";
			sql += "       CH3 * cCH3 + dCH3, ";
			sql += "       eEEpeNN * ceEEpeNN + deEEpeNN, ";
			sql += "       eEEmeNN  * ceEEmeNN  + deEEmeNN,  ";
			sql += "       e2EN * ce2EN + de2EN, ";
			sql += "       baro * cbaro + dbaro,  ";
			sql += "       rain * crain + drain,  ";
			sql += "       pore * cpore + dpore  ";
			sql += "FROM " + channel.getCode() + " a ";
			sql += "       INNER JOIN translations  b ON a.tid = b.tid ";
			sql += "       INNER JOIN ranks         c ON a.rid = c.rid ";
			sql += "WHERE  j2ksec >= ? ";
			sql += "AND    j2ksec <= ? ";
			
			// BEST POSSIBLE DATA QUERY
			if (ranks && rid != 0) {
				sql = sql + "AND   c.rid = ? ";
			}
			
			sql += "ORDER BY j2ksec ASC";
			
			if (ranks && rid == 0) {
				sql = sql + ", c.rank DESC";
			}
			
			if (ranks && rid != 0) {
				try{
					sql = getDownsamplingSQL(sql, "j2ksec", ds, dsInt);
				} catch (UtilException e){
					return getErrorResult("Can't downsample dataset: " + e.getMessage());
				}
			}
			
			if (maxrows != 0) {
				sql += " LIMIT " + (tempmaxrows + 1);
			}

			ps	= database.getPreparedStatement(sql);
			if(ds.equals(DownsamplingType.MEAN)){
				ps.setDouble(1, st);
				ps.setInt(2, dsInt);
				ps.setDouble(3, st);
				ps.setDouble(4, et);
				if (ranks && rid != 0) {
					ps.setInt(5, rid);
				}
			} else {
				ps.setDouble(1, st);
				ps.setDouble(2, et);
				if (ranks && rid != 0) {
					ps.setInt(3, rid);
				}
			}
			rs = ps.executeQuery();
		
			if (maxrows != 0 && getResultSetSize(rs) > tempmaxrows) { 
				return getErrorResult("Max rows (" + maxrows + " rows) for data source '" + vdxName + "' exceeded. Please use downsampling.");
			}
			
			double tempJ2ksec = Double.MAX_VALUE;
			
			// loop through each result and add to the list
			while (rs.next()) {
				
				// if this is a new j2ksec, then save this data, as it contains the highest rank
				if (Double.compare(tempJ2ksec, rs.getDouble(1)) != 0) {
					dataRow = new double[columnsReturned];
					for (int i = 0; i < columnsReturned; i++) {
						dataRow[i] = getDoubleNullCheck(rs, i+1);
					}
					pts.add(dataRow);
				}
				tempJ2ksec = rs.getDouble(1);
			}
			rs.close();
			
			if (pts.size() > 0) {
				return new BinaryResult(new TensorstrainData(pts));
			}
			
		} catch (Exception e) {
			logger.log(Level.SEVERE, "SQLTensorstrainDataSource.getTensorstrainData()", e);
			return null;
		}
		return null;
	}

	/**
	 * Get azimuths list in format "cid:azimuth" from database
	 * 
	 * @return List of Strings with : separated values
	 */
	public List<String> getAzimuths() {
		List<String> result = new ArrayList<String>();

		try {
			database.useDatabase(dbName);
			rs = database.getPreparedStatement("SELECT cid, natural_azimuth FROM channels ORDER BY cid").executeQuery();
			while (rs.next()) {
				result.add(String.format("%d:%f", rs.getInt(1), rs.getDouble(2)));
			}
			rs.close();

		} catch (Exception e) {
			logger.log(Level.SEVERE, "SQLTensorstrainDataSource.getAzimuths() failed.", e);
		}

		return result;
	}
	
	/**
	 * Get azimuth for channel
	 * @param channelCode	channel code
	 * @return azimuth of station, 0 for not found
	 */
	/*
	public double getNominalAzimuth (String channelCode) {
		double result = 0.0;
		
		try {
			database.useDatabase(dbName);
			ps = database.getPreparedStatement("SELECT natural_azimuth FROM channels WHERE code = ?");
			ps.setString(1, channelCode);
			rs = ps.executeQuery();
			rs.next();
			result = rs.getDouble(1);
			rs.close();
		} catch (Exception e) {
			logger.log(Level.SEVERE, "SQLTensorstrainDataSource.getNominalAzimuth() failed.", e);
		}
		return result;
	}
	*/
}
