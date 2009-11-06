package gov.usgs.vdx.data.hypo;

import gov.usgs.vdx.data.Column;
import gov.usgs.vdx.data.DataSource;
import gov.usgs.vdx.data.SQLDataSource;
import gov.usgs.vdx.db.VDXDatabase;
import gov.usgs.vdx.server.BinaryResult;
import gov.usgs.vdx.server.RequestResult;
import gov.usgs.vdx.server.TextResult;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;

/**
 * SQL Data Source for Hypocenter Data
 *
 * @author Dan Cervelli, Loren Antolik
 */
public class SQLHypocenterDataSource extends SQLDataSource implements DataSource {
	
	public static final String DATABASE_NAME	= "hypocenters";
	public static final boolean channels		= false;
	public static final boolean translations	= false;
	public static final boolean channelTypes	= false;
	public static final boolean ranks			= true;
	public static final boolean columns			= true;
	public static final Column[] DATA_COLUMNS	= new Column[] {
		new Column(1, "lon",	"Longitude",	"",	false, true),
		new Column(2, "lat",	"Latitude",		"",	false, true), 
		new Column(3, "depth",	"Depth",		"",	false, true), 
		new Column(4, "mag",	"Magnitude",	"",	false, true)};

	/**
	 * Get data source type, "hypocenters" for this class
	 */
	public String getType() { return DATABASE_NAME; }	
	public boolean getChannelsFlag() { return channels; }
	public boolean getTranslationsFlag() { return translations; }
	public boolean getChannelTypesFlag() { return channelTypes; }
	public boolean getRanksFlag() { return ranks; }
	public boolean getColumnsFlag() { return columns; }
	
	/**
	 * Initialize data source
	 */
	public void initialize(VDXDatabase db, String dbName) {
		defaultInitialize(db, dbName + "$" + getType());
		if (!databaseExists()) {
			createDatabase();
		}
	}
	
	/**
	 * Close database connection
	 */
	public void disconnect() {
		database.close();
	}

	/**
	 * Get flag if database exists
	 */
	public boolean databaseExists() {
		return defaultDatabaseExists();
	}
	
	/**
	 * Create hypocenters database
	 */
	public boolean createDatabase() {
		
		try {
			defaultCreateDatabase(channels, translations, channelTypes, ranks, columns);
			
			// columns table
			for (int i = 0; i < DATA_COLUMNS.length; i++) {
				defaultInsertColumn(DATA_COLUMNS[i]);
			}
			
			// channels false, translations false, ranks true, pre-defined data columns
			defaultCreateChannel(DATABASE_NAME, DATABASE_NAME, 0, 0, 0, channels, translations, ranks, columns);
			
			// alter the hypocenters table to use a different primary key
			database.useDatabase(dbName);
			database.getStatement().execute("ALTER TABLE " + DATABASE_NAME + " DROP PRIMARY KEY");
			database.getStatement().execute("ALTER TABLE " + DATABASE_NAME + " ADD PRIMARY KEY (j2ksec, lon, lat)");
			
			return true;
			
		} catch (Exception e) {
			logger.log(Level.SEVERE, "SQLHypocenterDataSource.createDatabase() failed.", e);
		}
		return false;
	}

	/**
	 * Getter for data. 
	 * Search value of 'action' parameter and retrieve corresponding data.
	 * @param command to execute, map of parameter-value pairs.
	 */	
	public RequestResult getData(Map<String, String> params) {
		
		String action = params.get("action");
		
		if (action == null) {
			return null;		

		} else if (action.equals("ranks")) {
			return new TextResult(defaultGetRanks());
			
		} else if (action.equals("data")) {
			int rid			= Integer.parseInt(params.get("rk"));
			double st		= Double.parseDouble(params.get("st"));
			double et		= Double.parseDouble(params.get("et"));
			double west		= Double.parseDouble(params.get("west"));
			double east		= Double.parseDouble(params.get("east"));
			double south	= Double.parseDouble(params.get("south"));
			double north	= Double.parseDouble(params.get("north"));
			double minDepth	= Double.parseDouble(params.get("minDepth"));
			double maxDepth	= Double.parseDouble(params.get("maxDepth"));
			double minMag	= Double.parseDouble(params.get("minMag"));
			double maxMag	= Double.parseDouble(params.get("maxMag"));	
			HypocenterList data = getHypocenterData(rid, st, et, west, east, south, north, minDepth, maxDepth, minMag, maxMag);
			if (data != null && data.size() > 0)
				return new BinaryResult(data);
		}
		return null;
	}
	
	/**
	 * Get Hypocenter data
	 * 
	 * @param rid		rank id
	 * @param st		start time
	 * @param et		end time
	 * @param west		west boundary
	 * @param east		east boundary
	 * @param south		south boundary
	 * @param north		north boundary
	 * @param minDepth	minimum depth
	 * @param maxDepth	maximum depth
	 * @param minMag	minimum magnitude
	 * @param maxMax	maximum magnitude
	 */
	public HypocenterList getHypocenterData(int rid, double st, double et, double west, double east, 
			double south, double north, double minDepth, double maxDepth, double minMag, double maxMag) {
		
		double[] dataRow;		
		List<Hypocenter> pts	= new ArrayList<Hypocenter>();
		HypocenterList result	= null;
		
		try {
			database.useDatabase(dbName);
			
			// build the sql
			sql  = "SELECT j2ksec, rid, lon, lat, depth, mag ";
			sql += "FROM   hypocenters ";
			sql += "WHERE  rid = ? ";
			sql += "AND    j2ksec >= ? AND j2ksec <= ? ";
			sql += "AND    lon    >= ? AND lon    <= ? ";
			sql += "AND    lat    >= ? AND lat    <= ? ";
			sql += "AND    depth  >= ? AND depth  <= ? ";
			sql += "AND    mag    >= ? AND mag    <= ? ";
			sql += "ORDER BY j2ksec ASC";
			
			ps = database.getPreparedStatement(sql);
			ps.setInt(1,rid);
			ps.setDouble(2, st);
			ps.setDouble(3, et);
			ps.setDouble(4, west);
			ps.setDouble(5, east);
			ps.setDouble(6, south);
			ps.setDouble(7, north);
			ps.setDouble(8, minDepth);
			ps.setDouble(9, maxDepth);
			ps.setDouble(10, minMag);
			ps.setDouble(11, maxMag);
			rs = ps.executeQuery();
			
			while (rs.next()) {
				dataRow = new double[] {rs.getDouble(1),rs.getDouble(2),rs.getDouble(3),rs.getDouble(4),rs.getDouble(5), rs.getDouble(6)};
				pts.add(new Hypocenter(dataRow));
			}
			rs.close();
			
			if (pts.size() > 0) {
				return new HypocenterList(pts);
			}
			
		} catch (Exception e) {
			logger.log(Level.SEVERE, "SQLHypocenterDataSource.getHypocenterData() failed.", e);
		}
		return result;
	}
	
	/**
	 * Insert data
	 * @param hc	Hypocenter
	 */
	public void insertHypocenter(Hypocenter hc) {
		
		try {
			database.useDatabase(dbName);
			sql = "INSERT IGNORE INTO hypocenters VALUES (?, ?, ?, ?, ?)";
			ps = database.getPreparedStatement(sql);
			ps.setDouble(1, hc.getTime());
			ps.setDouble(2, hc.getLon());
			ps.setDouble(3, hc.getLat());
			ps.setDouble(4, hc.getDepth());
			ps.setDouble(5, hc.getMag());
			ps.execute();
			
		} catch (Exception e) {
			logger.log(Level.SEVERE, "SQLHypocenterDataSource.insertHypocenter() failed.", e);
		}
	}
}
