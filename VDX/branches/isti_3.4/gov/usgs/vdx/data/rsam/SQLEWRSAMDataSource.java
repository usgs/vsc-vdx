package gov.usgs.vdx.data.rsam;

import gov.usgs.util.ConfigFile;
import gov.usgs.util.UtilException;
import gov.usgs.vdx.client.VDXClient.DownsamplingType;
import gov.usgs.vdx.data.Channel;
import gov.usgs.vdx.data.Column;
import gov.usgs.vdx.data.DataSource;
import gov.usgs.vdx.data.GenericDataMatrix;
import gov.usgs.vdx.data.SQLDataSource;
import gov.usgs.vdx.server.BinaryResult;
import gov.usgs.vdx.server.RequestResult;
import gov.usgs.vdx.server.TextResult;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;

import cern.colt.matrix.DoubleMatrix2D;

/**
 * SQL Data Source for RSAM Data
 *
 * @author Tom Parker, Loren Antolik
 */
public class SQLEWRSAMDataSource extends SQLDataSource implements DataSource {
	
	public static final String DATABASE_NAME	= "ewrsam";	
	public static final boolean channels		= true;
	public static final boolean translations	= false;
	public static final boolean channelTypes	= false;
	public static final boolean ranks			= false;
	public static final boolean columns			= true;
	public static final boolean menuColumns		= false;
	
	public static final Column[] DATA_COLUMNS	= new Column[] {
		new Column(1, "rsam",	"rsam",	"",	false, true, false)};
	private String tableSuffix;
	
	/**
	 * Constructor
	 */
	public SQLEWRSAMDataSource() {
		
	}
	
	/**
	 * Constructor
	 * @param s data type (Events/Values)
	 */
	public SQLEWRSAMDataSource(String s) {
		super();
		
		if (s.equals("Events"))
			tableSuffix = "_events";
		else if (s.equals("Values"))
			tableSuffix = "_values";
	}

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
	 * Get flag if database exist
	 */
	public boolean databaseExists() {
		return defaultDatabaseExists();
	}
	
	/**
	 * Create 'ewrsam' database
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
	 * Create entry in the channels table
	 * @param channelCode
	 * @param channelName
	 * @param lon
	 * @param lat
	 * @param height
	 * @return true if successful
	 */
	public boolean createChannel(String channelCode, String channelName, double lon, double lat, double height) {
		
		// create an entry in the channels table but don't build the table
		defaultCreateChannel(channelCode, channelName, lon, lat, height, 0, channels, translations, ranks, columns);
		
		// create a values and events table, and don't create an entry in the channels table
		defaultCreateChannel(channelCode + "_values", null, Double.NaN, Double.NaN, Double.NaN, 0, false, translations, ranks, columns);
		defaultCreateChannel(channelCode + "_events", null, Double.NaN, Double.NaN, Double.NaN, 0, false, translations, ranks, columns);
		
		return true;
	}
	
	/**
	 * Getter for data. 
	 * Search value of 'action' parameters and retrieve corresponding data.
	 * @param command to execute.
	 */
	public RequestResult getData(Map<String, String> params) {
		
		String action = params.get("action");
		
		if (action == null) {
			return null;
		
		} else if (action.equals("channels")) {
			return new TextResult(defaultGetChannels(channelTypes));
			
		} else if (action.equals("data")) {
			int cid			= Integer.parseInt(params.get("channel"));
			double st		= Double.parseDouble(params.get("st"));
			double et		= Double.parseDouble(params.get("et"));
			String plotType	= params.get("plotType");
			DownsamplingType ds = DownsamplingType.fromString(params.get("ds"));
			int dsInt		= Integer.parseInt(params.get("dsInt")); 
			RSAMData data = null;
			try{
				data = getEWRSAMData(cid, st, et, plotType, getMaxRows(), ds, dsInt);
			} catch (UtilException e){
				return getErrorResult(e.getMessage());
			}
			if (data != null) {
				return new BinaryResult(data);
			}
			
		} else if (action.equals("ewRsamMenu")) {
			return new TextResult(getTypes());
			
		} else if (action.equals("suppdata")) {
			return getSuppData( params, false );
		
		} else if (action.equals("metadata")) {
			return getMetaData( params, false );
			
		}
		return null;
	}

	/**
	 * Get 
	 * @return String List containing VALUES and/or EVENTS
	 */
	public List<String> getTypes() {
		
		List<String> result = new ArrayList<String>();
		
		try {
			database.useDatabase(dbName);
			
			// build the sql
			sql = "SHOW TABLES LIKE '%_events'";
			ps	= database.getPreparedStatement(sql);
			rs	= ps.executeQuery();
			if (rs.next()) {
				result.add("EVENTS");
			}
			
			// build the sql
			sql = "SHOW TABLES LIKE '%_values'";
			ps	= database.getPreparedStatement(sql);
			rs	= ps.executeQuery();
			if (rs.next()) {
				result.add("VALUES");
			}
			
		} catch (Exception e) {
			logger.log(Level.SEVERE, "SQLEWRSAMDataSource.getTypes()", e);
		}
		return result;
	}

	/**
	 * Get RSAM data
	 * @param cid	channel id
	 * @param st	start time
	 * @param et	end time
	 * @param type	data type (EVENTS/VALUES)
	 */
	public RSAMData getEWRSAMData(int cid, double st, double et, String plotType, int maxrows, DownsamplingType ds, int dsInt) throws UtilException {
		
		double[] dataRow;
		List<double[]> pts	= new ArrayList<double[]>();
		RSAMData result		= null;
		double count;
		
		try {
			database.useDatabase(dbName);
			
			// look up the channel code from the channels table, which is part of the name of the table to query
			Channel ch	= defaultGetChannel(cid, channelTypes);

			if (plotType.equals("VALUES")) {
				
				sql		= "SELECT j2ksec, rsam ";
				sql	   += "FROM   ?_values ";
				sql	   += "WHERE  j2ksec >= ? and j2ksec <= ? ";
				sql	   += "ORDER BY j2ksec";
				
				try{
					sql = getDownsamplingSQL(sql, "j2ksec", ds, dsInt);
				} catch (UtilException e){
					throw new UtilException("Can't downsample dataset: " + e.getMessage());
				}
				if(maxrows !=0){
					sql += " LIMIT " + (maxrows+1);
				}
				ps		= database.getPreparedStatement(sql);
				if(ds.equals(DownsamplingType.MEAN)){
					ps.setDouble(1, st);
					ps.setInt(2, dsInt);
					ps.setString(3, ch.getCode());
					ps.setDouble(4, st);
					ps.setDouble(5, et);
				} else {
					ps.setString(1, ch.getCode());
					ps.setDouble(2, st);
					ps.setDouble(3, et);
				}
				rs		= ps.executeQuery();
				if(maxrows !=0 && getResultSetSize(rs)> maxrows){ 
					throw new UtilException("Configured row count (" + maxrows + "rows) for source '" + dbName + "' exceeded. Please use downsampling.");
				}
				pts 	= new ArrayList<double[]>();
				
				// iterate through all results and create a double array to store the data
				while (rs.next()) {
					dataRow		= new double[2];
					dataRow[0]	= getDoubleNullCheck(rs, 1);
					dataRow[1]	= getDoubleNullCheck(rs, 2);
					pts.add(dataRow);
				}
				rs.close();
				
			} else if (plotType.equals("EVENTS")) {
				
				sql		= "SELECT j2ksec, rsam ";
				sql    += "FROM   ?_events ";
				sql	   += "WHERE  j2ksec >= ? and j2ksec <= ? and rsam != 0";
				if(maxrows !=0){
					sql += " LIMIT " + (maxrows+1);
				}
				
				ps		= database.getPreparedStatement(sql);
				ps.setString(1, ch.getCode());
				ps.setDouble(2, st);
				ps.setDouble(3, et);
				rs		= ps.executeQuery();
				if(maxrows !=0 && getResultSetSize(rs)> maxrows){ 
					throw new UtilException("Configured row count (" + maxrows + "rows) for source '" + dbName + "' exceeded.");
				}
				// setup the initial value
				count 		= 0;
				dataRow		= new double[2];				
				dataRow[0]	= st;
				dataRow[1]	= count;
				pts.add(dataRow);
			
				// this puts the data in the format 
				while (rs.next()) {
					double t = rs.getDouble(1);
					double c = rs.getDouble(2);
					for (int i = 0; i < c; i++) {
						dataRow		= new double[2];
						dataRow[0]	= t;
						dataRow[1]	= ++count;
						pts.add(dataRow);
					}
				}
				rs.close();
	
				// setup the final value
				dataRow		= new double[2];
				dataRow[0]	= et;
				dataRow[1]	= count;
				pts.add(dataRow);	
			}
			
			if (pts.size() > 0) {
				result	= new RSAMData(pts);
			}
			
		} catch (SQLException e) {
			logger.log(Level.SEVERE, "SQLEWRSAMDataSource.getEWRSAMData()", e);
		}
		return result;
	}
	
	/**
	 * Insert data into the database using the parameters passed
	 * @param channelCode
	 * @param gdm
	 * @param translations
	 * @param ranks
	 * @param rid
	 */
	public void insertData (String channelCode, GenericDataMatrix gdm, boolean translations, boolean ranks, int rid) {
		
		try {
			database.useDatabase(dbName);
			
			DoubleMatrix2D data = gdm.getData();
			
			if (true) {
				sql = "REPLACE INTO ";
			} else {
				sql = "INSERT IGNORE INTO ";
			}
			
			sql +=  channelCode + tableSuffix + " (j2ksec, rsam) VALUES (?,?)";
			ps	= database.getPreparedStatement(sql);
			for (int i=0; i < data.rows(); i++) {
				
				// i have no idea why this is happening
				if (i % 100 == 0) {
					System.out.print(".");
				}
				ps.setDouble(1, data.getQuick(i, 0));
				ps.setDouble(2, data.getQuick(i, 1));
				ps.execute();
			}
			
		} catch (Exception e) {
			logger.log(Level.SEVERE, "SQLEWRSAMDataSource.insertData()", e);
		}
	}
}