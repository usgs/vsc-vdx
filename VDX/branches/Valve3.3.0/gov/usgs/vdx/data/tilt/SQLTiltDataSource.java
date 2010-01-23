package gov.usgs.vdx.data.tilt;

import gov.usgs.util.ConfigFile;
import gov.usgs.util.Util;
import gov.usgs.vdx.data.Channel;
import gov.usgs.vdx.data.Column;
import gov.usgs.vdx.data.DataSource;
import gov.usgs.vdx.data.GenericDataMatrix;
import gov.usgs.vdx.data.SQLDataSource;
import gov.usgs.vdx.data.tilt.TiltData;
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
	
	private static final char MICRO = (char)0xb5;
	private static final char DEGREES = (char)0xb0;
	
	public static final String DATABASE_NAME	= "tilt";
	public static final boolean channels		= true;
	public static final boolean translations	= true;
	public static final boolean channelTypes	= false;
	public static final boolean ranks			= true;
	public static final boolean columns			= true;
	public static final boolean menuColumns		= true;
	
	public static final Column[] DATA_COLUMNS	= new Column[] {
		new Column(1, "xTilt",		"East",					MICRO + "R",	false, true),
		new Column(2, "yTilt",		"North",				MICRO + "R",	false, true), 
		new Column(3, "holeTemp",	"Hole Temperature",		DEGREES + "C",	false, true), 
		new Column(4, "boxTemp",	"Box Temperature",		DEGREES + "C",	false, true),
		new Column(5, "instVolt",	"Instrument Voltage",	"Volts",		false, true),
		new Column(6, "gndVolt",	"Ground Voltage",		"Volts",		false, true),
		new Column(7, "rain",		"Rainfall",				"cm",			false, true)};
	
	public static final Column[] MENU_COLUMNS	= new Column[] {
		new Column(1, "radial",		"Radial",				MICRO + "R",	true,	true),
		new Column(2, "tangetial",	"Tangetial",			MICRO + "R",	true,	true), 
		new Column(3, "xTilt",		"East",					MICRO + "R",	false,	true),
		new Column(4, "yTilt",		"North",				MICRO + "R",	false,	true), 
		new Column(5, "magnitude",	"Magnitude",			MICRO + "R",	false,	false),
		new Column(6, "azimuth",	"Azimuth",				MICRO + "R",	false,	false), 
		new Column(7, "holeTemp",	"Hole Temperature",		DEGREES + "C",	false,	false), 
		new Column(8, "boxTemp",	"Box Temperature",		DEGREES + "C",	false,	false),
		new Column(9, "instVolt",	"Instrument Voltage",	"Volts",		false,	false),
		new Column(10, "gndVolt",	"Ground Voltage",		"Volts",		false,	false),
		new Column(11, "rain",		"Rainfall",				"cm",			false,	false)};

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
	 * Create tilt database
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
		
		try {
			defaultCreateChannel(channelCode, channelName, lon, lat, height, tid, channels, translations, ranks, columns);
			
			// get the newly created channel id
			Channel ch = defaultGetChannel(channelCode, channelTypes);
			
			// update the channels table with the azimuth value
			database.useDatabase(dbName);
			ps = database.getPreparedStatement("UPDATE channels SET azimuth = ? WHERE cid = ?");
			ps.setDouble(1, azimuth);
			ps.setInt(2, ch.getCID());
			ps.execute();
			
			return true;
			
		} catch (Exception e) {
			logger.log(Level.SEVERE, "SQLTiltDataSource.createChannel() failed.", e);
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
			TiltData data	= getTiltData(cid, rid, st, et);
			if (data != null)
				return new BinaryResult(data);
		}
		return null;
	}

	/**
	 * Get Tilt Station data
	 * @param cid	channel id
	 * @param rid	rank id
	 * @param st	start time
	 * @param et	end time
	 * @return
	 */	
	public TiltData getTiltData(int cid, int rid, double st, double et) {
		
		double[] dataRow;		
		List<double[]> pts	= new ArrayList<double[]>();
		TiltData result		= null;
		int columnsReturned	= 0;
		double value;
		
		try {
			database.useDatabase(dbName);
			
			// look up the channel code from the channels table, which is the name of the table to query
			Channel channel	= defaultGetChannel(cid, channelTypes);
			columnsReturned	= 9;

			// build the sql
			sql  = "SELECT a.j2ksec, c.rid, ";
			sql += "       COS(RADIANS(azimuth))  * (xTilt * cxTilt + dxTilt) + SIN(RADIANS(azimuth)) * (yTilt * cyTilt + dyTilt), ";
			sql += "       -SIN(RADIANS(azimuth)) * (xTilt * cxTilt + dxTilt) + COS(RADIANS(azimuth)) * (yTilt * cyTilt + dyTilt), ";
			sql += "       holeTemp * choleTemp + dholeTemp, ";
			sql += "       boxTemp  * cboxTemp  + dboxTemp,  ";
			sql += "       instVolt * cinstVolt + dinstVolt, ";
			sql += "       gndVolt  * cgndVolt  + dgndVolt,  ";
			sql += "       rain     * crain     + drain      ";
			sql += "FROM " + channel.getCode() + " a ";
			sql += "       INNER JOIN translations  b ON a.tid = b.tid ";
			sql += "       INNER JOIN ranks         c ON a.rid = c.rid ";
			sql += "WHERE  j2ksec >= ? ";
			sql += "AND    j2ksec <= ? ";
			
			// BEST POSSIBLE DATA QUERY
			if (ranks && rid != 0) {
				sql = sql + "AND   c.rid = ? ";
			} else if (ranks && rid == 0) {
				sql = sql + "AND   c.rank = (SELECT MAX(e.rank) " +
				                            "FROM   " + channel.getCode() + " d, ranks e " +
				                            "WHERE  d.rid = e.rid  " +
				                            "AND    a.j2ksec = d.j2ksec) ";
			}
			
			sql += "ORDER BY j2ksec ASC";
			
			ps	= database.getPreparedStatement(sql);
			ps.setDouble(1, st);
			ps.setDouble(2, et);
			if (ranks && rid != 0) {
				ps.setInt(3, rid);
			}
			rs = ps.executeQuery();

			while (rs.next()) {
				dataRow = new double[columnsReturned];
				for (int i = 0; i < columnsReturned; i++) {
					value	= rs.getDouble(i + 1);
					if (rs.wasNull()) { value	= Double.NaN; }
					dataRow[i] = value;
				}
				pts.add(dataRow);
			}
			rs.close();
			
			if (pts.size() > 0) {
				return new TiltData(pts);
			}

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
	
	/**
	 * Insert data into the database using the parameters passed
	 * @param channelCode
	 * @param gdm
	 */
	public void insertData (String channelCode, GenericDataMatrix gdm, int rid) {
		defaultInsertData(channelCode, gdm, translations, ranks, rid);
	}
	
	public void insertV2Data (String code, double t, double x, double y, double h, double b, double i, double g, double r) {
		try {
			
			// default some variables
			int tid = -1;
			int oid = -1;
			int eid = -1;
			
			// set the database
			database.useV2Database("tilt");
			
			// get the translation and offset
            ps = database.getPreparedStatement("SELECT curTrans, curOffset, curEnv FROM stations WHERE code=?");
            ps.setString(1, code);
            rs = ps.executeQuery();
            if (rs.next()) {
            	tid = rs.getInt(1);
            	oid = rs.getInt(2);
            	eid = rs.getInt(3);
            }
            rs.close();
            
            // lower case the code because that's how the table names are in the database
            code.toLowerCase();

            // create the tilt entry
            ps = database.getPreparedStatement("INSERT IGNORE INTO " + code + "tilt VALUES (?,?,?,?,?,?,?,?)");
			ps.setDouble(1, t);
			ps.setString(2, Util.j2KToDateString(t));
			if (Double.isNaN(x)) { ps.setNull(3, 8); } else { ps.setDouble(3, x); }
			if (Double.isNaN(y)) { ps.setNull(4, 8); } else { ps.setDouble(4, y); }
			if (Double.isNaN(g)) { ps.setNull(5, 8); } else { ps.setDouble(5, g); }
			ps.setDouble(6, tid);
			ps.setDouble(7, oid);
			ps.setDouble(8, 0);
			ps.execute();
			
			// create the environment entry
            ps = database.getPreparedStatement("INSERT IGNORE INTO " + code + "env VALUES (?,?,?,?,?,?,?,?)");
			ps.setDouble(1, t);
			ps.setString(2, Util.j2KToDateString(t));
			if (Double.isNaN(h)) { ps.setNull(3, 8); } else { ps.setDouble(3, h); }
			if (Double.isNaN(b)) { ps.setNull(4, 8); } else { ps.setDouble(4, b); }
			if (Double.isNaN(i)) { ps.setNull(5, 8); } else { ps.setDouble(5, i); }
			if (Double.isNaN(r)) { ps.setNull(6, 8); } else { ps.setDouble(6, r); }
			if (Double.isNaN(g)) { ps.setNull(7, 8); } else { ps.setDouble(7, g); }
			ps.setDouble(8, eid);
			ps.execute();
			
		} catch (Exception e) {
			logger.log(Level.SEVERE, "SQLTiltDataSource.insertV2Data() failed.", e);
		}
	}
}
