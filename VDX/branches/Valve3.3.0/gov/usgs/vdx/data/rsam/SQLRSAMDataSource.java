package gov.usgs.vdx.data.rsam;

import gov.usgs.util.ConfigFile;
import gov.usgs.util.Util;
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
		new Column(1, "rsam",	"RSAM",	"RSAM",	false, true)};

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
	 * Get flag if database exist
	 */
	public boolean databaseExists() {
		return defaultDatabaseExists();
	}
	
	/**
	 * Create generic fixed database
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
	 * @param command to execute. 
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
			double period	= Util.stringToDouble(params.get("period"), 60.0);
			RSAMData data	= getRSAMData(cid, st, et, period);
			if (data != null) {
				return new BinaryResult(data);
			}
			
		} else if (action.equals("ratdata")) {
			String cids		= params.get("ch");
			double st		= Double.parseDouble(params.get("st"));
			double et		= Double.parseDouble(params.get("et"));
			double period	= Util.stringToDouble(params.get("period"), 60.0);
			RSAMData data	= getRatSAMData(cids, st, et, period);
			if (data != null) {
				return new BinaryResult(data);
			}
		}
		return null;
	}

	/**
	 * Get RSAM data
	 * @param cid	channel id
	 * @param rid	rank id
	 * @param st	start time
	 * @param et	end time
	 * @return 
	 */
	public RSAMData getRSAMData(int cid, double st, double et, double period) {

		double[] dataRow;
		List<double[]> pts	= new ArrayList<double[]>();
		RSAMData result		= null;
		
		try {
			database.useDatabase(dbName);
			
			// look up the channel code from the channels table, which is the name of the table to query
			Channel ch	= defaultGetChannel(cid, channelTypes);
			
			// build the sql
			sql = "SELECT MIN(j2ksec), AVG(rsam) ";
			sql+= "FROM   " + ch.getCode() + " ";
			sql+= "WHERE  j2ksec >= ? ";
			sql+= "AND    j2ksec <= ? ";
			sql+= "GROUP BY FLOOR(j2ksec / ?) ";
			sql+= "ORDER BY MIN(j2ksec)";
			ps	= database.getPreparedStatement(sql);
			ps.setDouble(1, st);
			ps.setDouble(2, et);
			ps.setDouble(3, period);
			rs	= ps.executeQuery();
			
			// iterate through all results and create a double array to store the data, index 1 is the j2ksec
			while (rs.next()) {
				dataRow		= new double[2];
				dataRow[0]	= rs.getDouble(1);
				dataRow[1]	= rs.getDouble(2);
				pts.add(dataRow);
			}
			rs.close();
			
			if (pts.size() > 0) {
				return new RSAMData(pts);
			}
			
		} catch (Exception e) {
			logger.log(Level.SEVERE, "SQLRSAMDataSource.getRSAMData()", e);
		}
		return result;
	}

	/**
	 * Get RatSAM data
	 * @param channel
	 * @param st
	 * @param et
	 * @param period
	 * @return
	 */
	public RSAMData getRatSAMData(String ch, double st, double et, double period) {
		RSAMData result1	= null;
		RSAMData result2	= null;
		
		String[] channels	= ch.split(",");
		int ch1				= Integer.valueOf(channels[0]);
		int ch2				= Integer.valueOf(channels[1]);
		result1				= getRSAMData(ch1, st, et, period);
		result2				= getRSAMData(ch2, st, et, period);
		
		return result1.getRatSAM(result2);
	}
}
