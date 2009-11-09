package gov.usgs.vdx.data.tilt;

import gov.usgs.util.Util;
import gov.usgs.vdx.data.Channel;
import gov.usgs.vdx.data.Column;
import gov.usgs.vdx.data.DataSource;
import gov.usgs.vdx.data.GenericDataMatrix;
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
 * SQL Data Source for Tilt Data
 *
 * @author Dan Cervelli, Loren Antolik
 */
public class SQLTiltStationDataSource extends SQLDataSource implements DataSource {
	
	public static final String DATABASE_NAME	= "tilt";
	public static final boolean channels		= true;
	public static final boolean translations	= true;
	public static final boolean channelTypes	= false;
	public static final boolean ranks			= true;
	public static final boolean columns			= true;
	public static final Column[] DATA_COLUMNS	= new Column[] {
		new Column(1, "xTilt",		"X Tilt",				"microradians",		false, true),
		new Column(2, "yTilt",		"Y Tilt",				"microradians",		false, true), 
		new Column(3, "holeTemp",	"Hole Temperature",		"degrees celsius",	false, true), 
		new Column(4, "boxTemp",	"Box Temperature",		"degrees celsius",	false, true),
		new Column(5, "instVolt",	"Instrument Voltage",	"Volts",			false, true),
		new Column(6, "gndVolt",	"Ground Voltage",		"Volts",			false, true),
		new Column(7, "rain",		"Rainfall",				"cm",				false, true)};
	
	/**
	 * Get data source type, tilt for this class
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
	 * Create tilt database
	 */
	public boolean createDatabase() {
		
		try {
			defaultCreateDatabase(channels, translations, channelTypes, ranks, columns);
			
			// columns table
			for (int i = 0; i < DATA_COLUMNS.length; i++) {
				defaultInsertColumn(DATA_COLUMNS[i]);
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
			logger.log(Level.SEVERE, "SQLTiltStationDataSource.createDatabase() failed.", e);
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
	public boolean createChannel(String channelCode, String channelName, double lon, double lat, double height, double azimuth) {
		
		try {
			defaultCreateChannel(channelCode, channelName, lon, lat, height, channels, translations, ranks, columns);
			
			// get the newly created channel id
			Channel ch = defaultGetChannel(channelCode, channelTypes);
			
			// update the channels table with the azimuth value
			database.useDatabase(dbName);
			ps = database.getPreparedStatement("UPDATE channels SET azimuth = ? WHERE cid = ?");
			ps.setDouble(1, azimuth);
			ps.setInt(2, ch.getId());
			ps.execute();
			
			return true;
			
		} catch (Exception e) {
			logger.log(Level.SEVERE, "SQLTiltStationDataSource.createChannel() failed.", e);
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
			
		} else if (action.equals("data")) {
			int cid		= Integer.parseInt(params.get("ch"));
			int rid		= Integer.parseInt(params.get("rk"));
			double st	= Double.parseDouble(params.get("st"));
			double et	= Double.parseDouble(params.get("et"));
			TiltStationData data = getTiltStationData(cid, rid, st, et);
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
	public TiltStationData getTiltStationData(int cid, int rid, double st, double et) {
		
		double[] dataRow;		
		List<double[]> pts		= new ArrayList<double[]>();
		TiltStationData result	= null;
		
		try {
			database.useDatabase(dbName);
			
			// look up the channel code from the channels table, which is the name of the table to query
			Channel ch	= defaultGetChannel(cid, channelTypes);

			// build the sql
			sql  = "SELECT j2ksec, ";
			sql += "       COS(RADIANS(azimuth))  * (xTilt * cxTilt + dxTilt) + SIN(RADIANS(azimuth)) * (yTilt * cyTilt + dyTilt), ";
			sql += "       -SIN(RADIANS(azimuth)) * (xTilt * cxTilt + dxTilt) + COS(RADIANS(azimuth)) * (yTilt * cyTilt + dyTilt), ";
			sql += "       holeTemp * choleTemp + dholeTemp, ";
			sql += "       boxTemp  * cboxTemp  + dboxTemp,  ";
			sql += "       instVolt * cinstVolt + dinstVolt, ";
			sql += "       gndVolt  * cgndVolt  + dgndVolt,  ";
			sql += "       rain     * crain     + drain      ";
			sql += "FROM   ? INNER JOIN translations ON ?.tid = translations.tid ";
			sql += "WHERE  j2ksec >= ? ";
			sql += "AND    j2ksec <= ? ";
			sql += "AND    rid = ? ";
			sql += "ORDER BY j2ksec ASC";
			
			ps	= database.getPreparedStatement(sql);
			ps.setString(1, ch.getName());
			ps.setString(2, ch.getName());
			ps.setDouble(3, st);
			ps.setDouble(4, et);
			ps.setInt(5, rid);
			rs = ps.executeQuery();

			while (rs.next()) {
				dataRow = new double[] { rs.getDouble(1), rs.getDouble(2), rs.getDouble(3), rs.getDouble(4), 
						rs.getDouble(5), rs.getDouble(6), rs.getDouble(7), rs.getDouble(8) };
				pts.add(dataRow);
			}
			rs.close();
			
			if (pts.size() > 0) {
				return new TiltStationData(pts);
			}

		} catch (Exception e) {
			logger.log(Level.SEVERE, "SQLTiltStationDataSource.getTiltData() failed.", e);
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
			ps = database.getPreparedStatement("SELECT azimuth FROM channels WHERE code=?");
			ps.setString(1, channelCode);
			rs = ps.executeQuery();
			rs.next();
			result = rs.getDouble(1);
			rs.close();
		} catch (Exception e) {
			logger.log(Level.SEVERE, "SQLTiltStationDataSource.getNominalAzimuth() failed.", e);
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
			logger.log(Level.SEVERE, "SQLTiltStationDataSource.insertV2Data() failed.", e);
		}
	}
}
