package gov.usgs.vdx.data.rsam;

import gov.usgs.math.DownsamplingType;
import gov.usgs.plot.data.RSAMData;
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
 * SQL Data Source for RSAM Data
 * 
 * @author Dan Cervelli, Loren Antolik
 */
public class SQLRSAMDataSource extends SQLDataSource implements DataSource {
	
	public static final String DATABASE_NAME	= "rsam";
	public static final boolean channels		= true;
	public static final boolean translations	= false;
	public static final boolean channelTypes	= false;
	public static final boolean ranks			= false;
	public static final boolean columns			= true;
	public static final boolean menuColumns		= false;
	
	public static final Column[] DATA_COLUMNS	= new Column[] {
		new Column(1, "rsam",	"RSAM",	"RSAM",	false, true, false)};

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
	 * Create generic fixed database
	 * @return true
	 */
	public boolean createDatabase() {
		defaultCreateDatabase(channels, translations, channelTypes, ranks, columns, menuColumns);
		
		// columns table
		for (int i = 0; i < DATA_COLUMNS.length; i++) {
			defaultInsertColumn(DATA_COLUMNS[i]);
		}
		
		return true;
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
		return defaultCreateChannel(channelCode, channelName, lon, lat, height, 0, channels, translations, ranks, columns);
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
			
		} else if (action.equals("data")) {
			int cid			= Integer.parseInt(params.get("ch"));
			double st		= Double.parseDouble(params.get("st"));
			double et		= Double.parseDouble(params.get("et"));
			DownsamplingType ds = DownsamplingType.fromString(params.get("ds"));
			int dsInt		= Integer.parseInt(params.get("dsInt")); 
			RSAMData data = null;
			try{
				data = getRSAMData(cid, st, et, getMaxRows(), ds, dsInt);
			} catch (UtilException e){
				return getErrorResult(e.getMessage());
			}	
			if (data != null) {
				return new BinaryResult(data);
			}
			
		} else if (action.equals("ratdata")) {
			String cids		= params.get("ch");
			double st		= Double.parseDouble(params.get("st"));
			double et		= Double.parseDouble(params.get("et"));
			DownsamplingType ds = DownsamplingType.fromString(params.get("ds"));
			int dsInt		= Integer.parseInt(params.get("dsInt")); 
			RSAMData data = null;
			try{
				data = getRatSAMData(cids, st, et, getMaxRows(), ds, dsInt);
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
	 * Get RSAM data
	 * @param cid	   channel id
	 * @param st	   start time
	 * @param et	   end time
	 * @param maxrows  maximum nbr of rows returned
	 * @param ds       type of downsampling
	 * @param dsInt    downsampling argument
	 * @return RSAM data
	 * @throws UtilException
	 */
	public RSAMData getRSAMData(int cid, double st, double et, int maxrows, DownsamplingType ds, int dsInt) throws UtilException {

		double[] dataRow;
		List<double[]> pts	= new ArrayList<double[]>();
		RSAMData result		= null;
		
		try {
			database.useDatabase(dbName);
			
			// look up the channel code from the channels table, which is the name of the table to query
			Channel ch	= defaultGetChannel(cid, channelTypes);
			
			// build the sql
			sql  = "SELECT j2ksec, rsam ";
			sql += "FROM   " + ch.getCode() + " ";
			sql += "WHERE  j2ksec >= ? ";
			sql += "AND    j2ksec <= ? ";
			sql += "ORDER BY j2ksec";
			
			sqlCount  = "SELECT COUNT(*) FROM (SELECT 1 ";
			sqlCount += sql.substring(sql.indexOf("FROM"));
						
			try{
				sql = getDownsamplingSQL(sql, "j2ksec", ds, dsInt);
			} catch (UtilException e){
				throw new UtilException("Can't downsample dataset: " + e.getMessage());
			}
			
			if(maxrows != 0){
				sql += " LIMIT " + (maxrows+1);
				
				// If the dataset has a maxrows paramater, check that the number of requested rows doesn't
				// exceed that number prior to running the full query. This can save a decent amount of time
				// for large queries. Note that this only applies for non-downsampled queries. This is done for
				// two reasons: 1) If the user is downsampling, they already know they're dealing with a lot of data
				// and 2) the way MySQL handles the multiple nested queries that would result makes it slower than
				// just doing the full query to begin with.
				if(ds.equals(DownsamplingType.NONE)) {
					ps = database.getPreparedStatement(sqlCount + " LIMIT " + (maxrows + 1) + ") as T");
					ps.setDouble(1, st);
					ps.setDouble(2, et);
					rs = ps.executeQuery();
					if(rs.next() && rs.getInt(1) > maxrows)
						throw new UtilException("Max rows (" + maxrows + " rows) for data source '" + vdxName + "' exceeded. Please use downsampling.");
				}
			}
			
			ps	= database.getPreparedStatement(sql);
			if(ds.equals(DownsamplingType.MEAN)){
				ps.setDouble(1, st);
				ps.setInt(2, dsInt);
				ps.setDouble(3, st);
				ps.setDouble(4, et);
			} else {
				ps.setDouble(1, st);
				ps.setDouble(2, et);
			}
			rs	= ps.executeQuery();
			
			// Check for the amount of data returned in a downsampled query. Non-downsampled queries are checked above.
			if (!ds.equals(DownsamplingType.NONE) && maxrows != 0 && getResultSetSize(rs) > maxrows) { 
				throw new UtilException("Max rows (" + maxrows + " rows) for source '" + vdxName + "' exceeded. Please downsample further.");
			}

			// iterate through all results and create a double array to store the data, index 1 is the j2ksec
			while (rs.next()) {
				dataRow		= new double[2];
				dataRow[0]	= getDoubleNullCheck(rs, 1);
				dataRow[1]	= getDoubleNullCheck(rs, 2);
				pts.add(dataRow);
			}
			rs.close();
			
			if (pts.size() == 0) {
				dataRow		= new double[2];
				dataRow[0]	= Double.NaN;
				dataRow[1]	= Double.NaN;
				pts.add(dataRow);
			}
			
			result = new RSAMData(pts);
			
		} catch (Exception e) {
			logger.log(Level.SEVERE, "SQLRSAMDataSource.getRSAMData()", e);
		}
		
		return result;
	}

	/**
	 * Get RatSAM data
	 * @param ch
	 * @param st
	 * @param et
	 * @param maxrows  maximum nbr of rows returned
	 * @param ds       type of downsampling
	 * @param dsInt    downsampling argument
	 * @return RatSAM data
	 * @throws UtilException
	 */
	public RSAMData getRatSAMData(String ch, double st, double et, int maxrows, DownsamplingType ds, int dsInt) throws UtilException {
		RSAMData result1	= null;
		RSAMData result2	= null;
		
		String[] channels	= ch.split(",");
		int ch1				= Integer.valueOf(channels[0]);
		int ch2				= Integer.valueOf(channels[1]);
		result1				= getRSAMData(ch1, st, et, maxrows, ds, dsInt);
		result2				= getRSAMData(ch2, st, et, maxrows, ds, dsInt);
		
		return result1.getRatSAM(result2);
	}
}
