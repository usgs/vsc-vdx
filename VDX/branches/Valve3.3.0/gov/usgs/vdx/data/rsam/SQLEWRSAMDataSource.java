package gov.usgs.vdx.data.rsam;

import gov.usgs.util.ConfigFile;
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
	public static final boolean plotColumns		= false;
	
	public static final Column[] DATA_COLUMNS	= new Column[] {
		new Column(1, "rsam",	"rsam",	"",	false, true)};
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
	 */
	public boolean databaseExists() {
		return defaultDatabaseExists();
	}
	
	/**
	 * Create 'ewrsam' database
	 */
	public boolean createDatabase() {
		defaultCreateDatabase(channels, translations, channelTypes, ranks, columns, plotColumns);
		
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
		defaultCreateChannel(channelCode, channelName, lon, lat, height, channels, translations, ranks, columns);
		
		// create a values and events table, and don't create an entry in the channels table
		defaultCreateChannel(channelCode + "_values", "", 0, 0, 0, false, translations, ranks, columns);
		defaultCreateChannel(channelCode + "_events", "", 0, 0, 0, false, translations, ranks, columns);
		
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
			int p			= Integer.parseInt(params.get("period"));
			String plotType	= params.get("plotType");
			RSAMData data	= getEWRSAMData(cid, st, et, p, plotType);
			if (data != null) {
				return new BinaryResult(data);
			}
			
		} else if (action.equals("ewRsamMenu")) {
			return new TextResult(getTypes());
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
	 * @param p		period
	 * @param type	data type (EVENTS/VALUES)
	 */
	public RSAMData getEWRSAMData(int cid, double st, double et, int p, String plotType) {
		
		double[] dataRow;
		List<double[]> pts	= new ArrayList<double[]>();
		RSAMData result		= null;
		double count;
		
		try {
			database.useDatabase(dbName);
			
			// look up the channel code from the channels table, which is part of the name of the table to query
			Channel ch	= defaultGetChannel(cid, channelTypes);

			if (plotType.equals("VALUES")) {
				
				sql		= "SELECT j2ksec + ? / 2, avg(rsam) ";
				sql	   += "FROM   ?_values ";
				sql	   += "WHERE  j2ksec >= ? and j2ksec <= ? ";
				sql	   += "GROUP BY FLOOR(j2ksec / ?) ";
				ps		= database.getPreparedStatement(sql);
				ps.setDouble(1, p);
				ps.setString(2, ch.getCode());
				ps.setDouble(3, st);
				ps.setDouble(4, et);
				ps.setDouble(5, p);
				rs		= ps.executeQuery();
				pts 	= new ArrayList<double[]>();
				
				// iterate through all results and create a double array to store the data
				while (rs.next()) {
					dataRow		= new double[2];
					dataRow[0]	= rs.getDouble(1);
					dataRow[1]	= rs.getDouble(2);
					pts.add(dataRow);
				}
				rs.close();
				
			} else if (plotType.equals("EVENTS")) {
				
				sql		= "SELECT j2ksec, rsam ";
				sql    += "FROM   ?_events ";
				sql	   += "WHERE  j2ksec >= ? and j2ksec <= ? and rsam != 0";
				ps		= database.getPreparedStatement(sql);
				ps.setString(1, ch.getCode());
				ps.setDouble(2, st);
				ps.setDouble(3, et);
				rs		= ps.executeQuery();
				
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
				// result = new EWRSAMData(pts, events);
				result	= new RSAMData(pts);
			}
			
		} catch (Exception e) {
			logger.log(Level.SEVERE, "SQLEWRSAMDataSource.getEWRSAMData()", e);
		}
		return result;
	}
	
	/**
	 * Insert data for channel	 * 
	 * @param channelCode	channel code
	 * @param data			data matrix to insert
	 * @param r				if we permit data replacing?
	 */
	public void insertData(String channelCode, DoubleMatrix2D data, boolean r) {
		
		try {
			database.useDatabase(dbName);
			
			if (r) {
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