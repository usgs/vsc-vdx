package gov.usgs.vdx.data.tilt;

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
 * SQL Data Source for Tilt Data
 *
 * @author Dan Cervelli, Loren Antolik
 */
public class SQLTiltDataSource extends SQLDataSource implements DataSource {
	
	public static final String DATABASE_NAME	= "tilt";
	public static final boolean channels		= true;
	public static final boolean translations	= true;
	public static final boolean channelTypes	= false;
	public static final boolean ranks			= true;
	public static final boolean columns			= true;
	public static final boolean menuColumns		= true;
	
	public static final Column[] DATA_COLUMNS	= new Column[] {
		new Column(1, "xTilt",		"East",				"microrad",	false, true, false),
		new Column(2, "yTilt",		"North",			"microrad",	false, true, false), 
		new Column(3, "holeTemp",	"Hole Temp.",		"celsius",  false, true, true), 
		new Column(4, "boxTemp",	"Box Temp.",		"celsius",  false, true, true),
		new Column(5, "instVolt",	"Inst. Voltage",	"volts",	false, true, true),
		new Column(6, "gndVolt",	"Gnd. Voltage",		"volts",	false, true, false),
		new Column(7, "rainfall",	"Rainfall",			"mm",		false, true, true)};

	public static final Column[] MENU_COLUMNS	= new Column[] {
		new Column(1, "radial",		"Radial",			"microrad",	true,	true,  false),
		new Column(2, "tangential",	"Tangential",		"microrad",	true,	true,  false), 
		new Column(3, "xTilt",		"East",				"microrad",	false,	true,  false),
		new Column(4, "yTilt",		"North",			"microrad",	false,	true,  false), 
		new Column(5, "magnitude",	"Magnitude",		"microrad",	false,	true,  false),
		new Column(6, "azimuth",	"Azimuth",			"microrad",	false,	true,  false), 
		new Column(7, "holeTemp",	"Hole Temp.",		"celsius",  false,	true,  true), 
		new Column(8, "boxTemp",	"Box Temp.",		"celsius",  false,	true,  true),
		new Column(9, "instVolt",	"Inst. Voltage",	"volts",	false,	true,  true),
		new Column(10, "rainfall",	"Rainfall",			"mm",		false,	true,  true)};

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
	 * @return true if database exists, false otherwise
	 */
	public boolean databaseExists() {
		return defaultDatabaseExists();
	}
	
	/**
	 * Create tilt database
	 * @return true if successful, false otherwise
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
			
			// alter the channels table to add an azimuth column.  alter the translations table to add an azimuth column
			database.useDatabase(dbName);
			st = database.getStatement();
			st.execute("ALTER TABLE channels	 ADD azimuth DOUBLE DEFAULT 0");
			st.execute("ALTER TABLE translations ADD azimuth DOUBLE DEFAULT 0");
			
			return true;
			
		} catch (Exception e) {
			logger.log(Level.SEVERE, "SQLTiltDataSource.createDatabase() failed.", e);
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
	 * @param azimuth		azimuth of the deformation source
	 * @return true if successful
	 */	
	public boolean createChannel(String channelCode, String channelName, double lon, double lat, double height, int tid, double azimuth) {
		Channel channel = new Channel(0, channelCode, channelName, lon, lat, azimuth, height);
		return defaultCreateTiltChannel(channel, tid, azimuth, channels, translations, ranks, columns);
	}

	/**
	 * Getter for data. 
	 * Search value of 'action' parameter and retrieve corresponding data.
	 * @param params command to execute, map of parameter-value pairs.
	 * @return request result
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
			return getTiltData(cid, rid, st, et, getMaxRows(), ds, dsInt);
			
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
	 * Get Tilt Station data
	 * @param cid	  channel id
	 * @param rid	  rank id
	 * @param st	  start time
	 * @param et	  end time
	 * @param maxrows maximum number of rows returned
	 * @param ds      downsampling type
	 * @param dsInt   downsampling argument
	 * @return request result
	 */	
	public RequestResult getTiltData(int cid, int rid, double st, double et, int maxrows, DownsamplingType ds, int dsInt) {
		
		double[] dataRow;		
		List<double[]> pts	= new ArrayList<double[]>();
		BinaryResult result	= null;
		int columnsReturned	= 0;
		
		try {
			
			database.useDatabase(dbName);
			
			// look up the channel code from the channels table, which is the name of the table to query
			Channel channel	= defaultGetChannel(cid, channelTypes);
			columnsReturned	= 8;
			
			// calculate the num of rows to limit the query to
			int tempmaxrows;
			if (rid != 0) {
				tempmaxrows = maxrows;
			} else {
				tempmaxrows = maxrows * defaultGetNumberOfRanks();
			}

			// build the sql
			sql  = "SELECT j2ksec, c.rid, ";
			sql += "       COS(RADIANS(b.azimuth))  * (xTilt * cxTilt + dxTilt) + SIN(RADIANS(b.azimuth)) * (yTilt * cyTilt + dyTilt), ";
			sql += "       -SIN(RADIANS(b.azimuth)) * (xTilt * cxTilt + dxTilt) + COS(RADIANS(b.azimuth)) * (yTilt * cyTilt + dyTilt), ";
			sql += "       holeTemp * choleTemp + dholeTemp, ";
			sql += "       boxTemp  * cboxTemp  + dboxTemp,  ";
			sql += "       instVolt * cinstVolt + dinstVolt, ";
			sql += "       rainfall * crainfall + drainfall  ";
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
				try {
					sql = getDownsamplingSQL(sql, "j2ksec", ds, dsInt);
				} catch (UtilException e) {
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
			
			if (pts.size() == 0) {
				dataRow = new double[columnsReturned];
				for (int i = 0; i < columnsReturned; i++) {
					dataRow[i] = Double.NaN;
				}
				pts.add(dataRow);
			}
			
			result = new BinaryResult(new TiltData(pts));
			
		} catch (Exception e) {
			logger.log(Level.SEVERE, "SQLTiltDataSource.getTiltData()", e);
		}
		return result;
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
			rs = database.getPreparedStatement("SELECT cid, azimuth FROM channels ORDER BY cid").executeQuery();
			while (rs.next()) {
				result.add(String.format("%d:%f", rs.getInt(1), rs.getDouble(2)));
			}
			rs.close();

		} catch (Exception e) {
			logger.log(Level.SEVERE, "SQLTiltDataSource.getAzimuths() failed.", e);
		}

		return result;
	}
	
	/**
	 * Get azimuth for channel
	 * @param channelCode	channel code
	 * @return azimuth of station, 0 for not found
	 */
	public double getNominalAzimuth (String channelCode) {
		double result = 0.0;
		
		try {
			database.useDatabase(dbName);
			ps = database.getPreparedStatement("SELECT azimuth FROM channels WHERE code = ?");
			ps.setString(1, channelCode);
			rs = ps.executeQuery();
			rs.next();
			result = rs.getDouble(1);
			rs.close();
		} catch (Exception e) {
			logger.log(Level.SEVERE, "SQLTiltDataSource.getNominalAzimuth() failed.", e);
		}
		return result;
	}
}
