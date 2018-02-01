package gov.usgs.volcanoes.vdx.data.gps;

import gov.usgs.volcanoes.core.math.DownsamplingType;
import gov.usgs.volcanoes.core.configfile.ConfigFile;
import gov.usgs.volcanoes.core.util.UtilException;
import gov.usgs.volcanoes.vdx.data.Channel;
import gov.usgs.volcanoes.vdx.data.Column;
import gov.usgs.volcanoes.vdx.data.DataSource;
import gov.usgs.volcanoes.vdx.data.Rank;
import gov.usgs.volcanoes.vdx.data.SQLDataSource;
import gov.usgs.volcanoes.vdx.server.BinaryResult;
import gov.usgs.volcanoes.vdx.server.RequestResult;
import gov.usgs.volcanoes.vdx.server.TextResult;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * SQL Data Source for GPS Data
 * 
 * @author Dan Cervelli
 * @author Loren Antolik
 * @author Bill Tollett
 */
public class SQLGPSDataSource extends SQLDataSource implements DataSource {

	private static final Logger LOGGER = LoggerFactory.getLogger(SQLGPSDataSource.class);

	public static final String DATABASE_NAME	= "gps";
	public static final boolean channels		= true;
	public static final boolean translations	= false;
	public static final boolean channelTypes	= true;
	public static final boolean ranks			= true;
	public static final boolean columns			= false;
	public static final boolean menuColumns		= true;
	
	public static final Column[] MENU_COLUMNS	= new Column[] {
		new Column(1, "east",	"East",		"meters",	true,  false, false),
		new Column(2, "north",	"North",	"meters",	true,  false, false), 
		new Column(3, "up",		"Up",		"meters",	true,  false, false),
		new Column(4, "length",	"Length",	"meters",	false, false, false)};

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
	 * Create 'gps' database
	 * @return true if successful, false otherwise
	 */
	public boolean createDatabase() {
		
		try {
			defaultCreateDatabase(channels, translations, channelTypes, ranks, columns, menuColumns);
			
			// menu columns table
			for (int i = 0; i < MENU_COLUMNS.length; i++) {
				defaultInsertMenuColumn(MENU_COLUMNS[i]);
			}
			
			// create solutions and sources tables that are unique to the gps schema
			database.useDatabase(dbName);
			st = database.getStatement();
			st.execute(
					"CREATE TABLE sources (sid INT AUTO_INCREMENT," +
					"name VARCHAR(255), hash VARCHAR(32)," +
					"j2ksec0 DOUBLE NOT NULL, j2ksec1 DOUBLE NOT NULL," + 
					"rid INT," +
					"PRIMARY KEY (sid, j2ksec0, j2ksec1)," +
					"UNIQUE KEY (name, j2ksec0, j2ksec1, rid)," +
					"KEY index_j2ksec0 (j2ksec0), KEY index_j2ksec1 (j2ksec1))");
			st.execute(
					"CREATE TABLE solutions (sid INT, cid INT," +
					"x DOUBLE, y DOUBLE, z DOUBLE," +
					"sxx DOUBLE, syy DOUBLE, szz DOUBLE," +
					"sxy DOUBLE, sxz DOUBLE, syz DOUBLE," +
					"PRIMARY KEY (sid, cid))");
			
			LOGGER.info("SQLGPSDataSource.createDatabase({}_{}) succeeded.",
					database.getDatabasePrefix(), dbName);
			return true;

		} catch (Exception e) {
			LOGGER.error("SQLGPSDataSource.createDatabase({}_{}) failed.",
					database.getDatabasePrefix(), dbName, e);
		}

		return false;
	}
	
	/**
	 * Create entry in the channels table
	 * @param channelCode
	 * @param channelName
	 * @param lon
	 * @param lat
	 * @param height
	 * @param active
	 * @return true if successful
	 */
	public boolean createChannel(String channelCode, String channelName, double lon, double lat, double height, int active) {
		return defaultCreateChannel(channelCode, channelName, lon, lat, height, active, 0, channels, translations, ranks, columns);
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
			
		} else if (action.equals("channelTypes")) {
			return new TextResult(defaultGetChannelTypes());
			
		} else if (action.equals("ranks")) {
			return new TextResult(defaultGetRanks());			

		} else if (action.equals("columns")) {
			return new TextResult(defaultGetMenuColumns(menuColumns));
			
		} else if (action.equals("data")) {
			int cid			= Integer.parseInt(params.get("ch"));
			int rid			= Integer.parseInt(params.get("rk"));
			double st		= Double.parseDouble(params.get("st"));
			double et		= Double.parseDouble(params.get("et"));
			DownsamplingType ds = DownsamplingType.fromString(params.get("ds"));
			int dsInt		= Integer.parseInt(params.get("dsInt")); 
			GPSData data = null;
			try{
				data = getGPSData(cid, rid, st, et, getMaxRows(), ds, dsInt);
			} catch (UtilException e){
				return getErrorResult(e.getMessage());
			}	
			if (data != null) {
				return new BinaryResult(data);
			}

		} else if (action.equals("supptypes")) {
			return getSuppTypes( true );
		
		} else if (action.equals("suppdata")) {
			return getSuppData( params, true );
		
		} else if (action.equals("metadata")) {
			return getMetaData( params, true );

		}
		return null;
	}
	
	/**
	 * Get channel from database
	 * @param channelCode	channel code
	 * @return channel
	 */
	public Channel getChannel(String channelCode) {
		return defaultGetChannel(channelCode, channelTypes);
	}

	/**
	 * Get List of channels list from database
	 * @return List of channels
	 */
	public List<Channel> getChannelsList() {
		return defaultGetChannelsList(channelTypes);
	}
	
	/**
	 * Get GPSData
	 * @param cid	channel id
	 * @param rid	rank id
	 * @param st	start time
	 * @param et	end time
	 * @return GPSData
	 */
	public GPSData getGPSData(int cid, int rid, double st, double et, int maxrows, DownsamplingType ds, int dsInt) throws UtilException {
		
		DataPoint dp;
		GPSData result = null;
		
		try {
			
			database.useDatabase(dbName);
			List<DataPoint> dataPoints = new ArrayList<DataPoint>();
			
			// calculate the num of rows to limit the query to
			int tempmaxrows;
			if (rid != 0) {
				tempmaxrows = maxrows;
			} else {
				tempmaxrows = maxrows * defaultGetNumberOfRanks();
			}
			
			sql  = "SELECT (j2ksec0 + j2ksec1) / 2, d.rid, x, y, z, sxx, syy, szz, sxy, sxz, syz ";
			sql	+= "FROM   solutions a " +
				   "INNER JOIN channels b ON a.cid = b.cid " +
				   "INNER JOIN sources  c ON a.sid = c.sid " +
				   "INNER JOIN ranks    d ON c.rid = d.rid " +
				   "WHERE  b.cid    = ? " +
				   "AND    (c.j2ksec0 + c.j2ksec1) / 2 >= ? " +
				   "AND    (c.j2ksec0 + c.j2ksec1) / 2 <= ? ";
			
			sqlCount = "SELECT COUNT(*) FROM (SELECT 1 FROM solutions a " +
					   "INNER JOIN channels b ON a.cid = b.cid " + 
					   "INNER JOIN sources c ON a.sid = c.sid " + 
					   "INNER JOIN ranks d ON c.rid = d.rid " +
					   "WHERE b.cid = ? " +
					   "AND (c.j2ksec0 + c.j2ksec1) / 2 >= ? " +
					   "AND (c.j2ksec0 + c.j2ksec1) / 2 <= ? ";
			
			if (rid != 0) {
				sql 	 += "AND   d.rid = ? ";
				sqlCount += "AND d.rid = ? ";
			}
			
			sql		 += "ORDER BY 1 ASC, d.rank DESC";
			sqlCount += "ORDER BY 1 ASC, d.rank DESC";
			
			if (rid != 0) {
				try {
					sql = getDownsamplingSQL(sql, "(j2ksec0 + j2ksec1) / 2", ds, dsInt);
				} catch (UtilException e) {
					throw new UtilException("Can't downsample dataset: " + e.getMessage());
				}
			}
			
			if (maxrows != 0) {
				sql += " LIMIT " + (tempmaxrows + 1);
				
				// If the dataset has a maxrows paramater, check that the number of requested rows doesn't
				// exceed that number prior to running the full query. This can save a decent amount of time
				// for large queries. Note that this only applies for non-downsampled queries. This is done for
				// two reasons: 1) If the user is downsampling, they already know they're dealing with a lot of data
				// and 2) the way MySQL handles the multiple nested queries that would result makes it slower than
				// just doing the full query to begin with.
				if(ds.equals(DownsamplingType.NONE)) {
					ps = database.getPreparedStatement(sqlCount + " LIMIT " + (tempmaxrows + 1) + ") as T");
					ps.setInt(1, cid);
					ps.setDouble(2, st);
					ps.setDouble(3, et);
					if (rid != 0) {
						ps.setInt(4, rid);
					}
					rs = ps.executeQuery();
					if (rs.next() && rs.getInt(1) > tempmaxrows)
						throw new UtilException("Max rows (" + maxrows + " rows) for data source '" + vdxName + "' exceeded. Please use downsampling.");
				}
			}

			ps = database.getPreparedStatement(sql);
			if(ds.equals(DownsamplingType.MEAN)){
				ps.setDouble(1, st);
				ps.setInt(2, dsInt);
				ps.setInt(3, cid);
				ps.setDouble(4, st);
				ps.setDouble(5, et);
				if (rid != 0) {
					ps.setInt(6, rid);
				}
			} else {
				ps.setInt(1, cid);
				ps.setDouble(2, st);
				ps.setDouble(3, et);
				if (rid != 0) {
					ps.setInt(4, rid);
				}
			}
			rs = ps.executeQuery();
			
			// Check for the amount of data returned in a downsampled query. Non-downsampled queries are checked above.
			if (!ds.equals(DownsamplingType.NONE) && maxrows != 0 && getResultSetSize(rs) > tempmaxrows) { 
				throw new UtilException("Max rows (" + maxrows + " rows) for source '" + vdxName + "' exceeded. Please downsample further.");
			}
			
			double tempJ2ksec = Double.MAX_VALUE;
			
			// loop through each result and add to the list
			while (rs.next()) {
				
				// if this is a new j2ksec, then save this data, as it contains the highest rank
				if (Double.compare(tempJ2ksec, rs.getDouble(1)) != 0) {
					dp		= new DataPoint();
					dp.t	= getDoubleNullCheck(rs, 1);
					dp.r	= getDoubleNullCheck(rs, 2);
					dp.x	= getDoubleNullCheck(rs, 3);
					dp.y	= getDoubleNullCheck(rs, 4);
					dp.z	= getDoubleNullCheck(rs, 5);
					dp.sxx	= getDoubleNullCheck(rs, 6);
					dp.syy	= getDoubleNullCheck(rs, 7);
					dp.szz	= getDoubleNullCheck(rs, 8);
					dp.sxy	= getDoubleNullCheck(rs, 9);
					dp.sxz	= getDoubleNullCheck(rs, 10);
					dp.syz	= getDoubleNullCheck(rs, 11);
					dataPoints.add(dp);
				}
				tempJ2ksec = rs.getDouble(1);
			}
			rs.close();
			
			if (dataPoints.size() == 0) {
				dp		= new DataPoint();
				dp.t	= Double.NaN;
				dp.r	= Double.NaN;
				dp.x	= Double.NaN;
				dp.y	= Double.NaN;
				dp.z	= Double.NaN;
				dp.sxx	= Double.NaN;
				dp.syy	= Double.NaN;
				dp.szz	= Double.NaN;
				dp.sxy	= Double.NaN;
				dp.sxz	= Double.NaN;
				dp.syz	= Double.NaN;
				dataPoints.add(dp);
			}
			
			result = new GPSData(dataPoints);
			
		} catch (SQLException e) {
			LOGGER.error("SQLGPSDataSource.getGPSData({},{},{},{}) failed.", cid, rid, st, et, e);
		}
		
		return result;
	}
	
	
	/**
	 * Insert a source file entry to the database.
	 * If the filename/rank combination already exist then they are first deleted to allow for an overwrite
	 * @param name	name of the file
	 * @param hash	md5 hash code of the file
	 * @param t0	start time
	 * @param t1	end time
	 * @param rid	rank id
	 * @return source id on success, -1 on failure
	 */
	public int insertSource(String name, String hash, double t0, double t1, int rid) {
		int sid = -1;
		
		try {
			database.useDatabase(dbName);
			
			// lookup this hash. if it exists then this file has already been imported
			ps = database.getPreparedStatement("SELECT sid FROM sources WHERE hash = ? ");
			ps.setString(1, hash);
			rs = ps.executeQuery();
			if (rs.next()) {
				sid = rs.getInt(1);
			}
			rs.close();
			if (sid > 0) {
				return -1;
			}
			
			// lookup this filename/rank combination.  if it exists then delete it from the database
			ps = database.getPreparedStatement("SELECT sid FROM sources WHERE j2ksec0 = ? AND j2ksec1 = ? AND rid = ?");
			ps.setDouble(1, t0);
			ps.setDouble(2, t1);
			ps.setInt(3, rid);
			rs = ps.executeQuery();
			if (rs.next()) {
				Rank rank;
				int delcount;
				sid			= rs.getInt(1);
				ps			= database.getPreparedStatement("DELETE FROM solutions WHERE sid = ?");
				ps.setInt(1, sid);
				delcount	= ps.executeUpdate();
				ps			= database.getPreparedStatement("DELETE FROM sources WHERE sid = ?");
				ps.setInt(1, sid);
				ps.executeUpdate();
				rank		= defaultGetRank(rid);
				LOGGER.error("deleted {} rank {} ({} solutions)", name, rank.getName(), delcount);
			}
			
			// it is now safe to insert this NEW source
			ps = database.getPreparedStatement("INSERT INTO sources (name, hash, j2ksec0, j2ksec1, rid) VALUES (?,?,?,?,?)");
			ps.setString(1, name);
			ps.setString(2, hash);
			ps.setDouble(3, t0);
			ps.setDouble(4, t1);
			ps.setInt(5, rid);
			ps.execute();
			rs = database.getPreparedStatement("SELECT LAST_INSERT_ID()").executeQuery();
			if (rs.next()) {
				sid = rs.getInt(1);
			}
			rs.close();
			
			return sid;
			
		} catch (Exception e) {
			LOGGER.error("SQLGPSDataSource.insertSource() failed.", e);
		}
		
		return sid;
	}
	
	/**
	 * Insert a source file entry to the database.
	 * If the filename/rank combination already exist then they are first deleted to allow for an overwrite
	 * @param name	name of the file
	 * @param t0	start time
	 * @param t1	end time
	 * @param rid	rank id
	 * @return source id on success, -1 on failure
	 */
	public int insertSourceSimple(String name, double t0, double t1, int rid) {
		int sid = -1;
		
		try {
			database.useDatabase(dbName);
			
			// lookup this filename/rank combination.  if it exists then delete it from the database
			ps = database.getPreparedStatement("SELECT sid FROM sources WHERE j2ksec0 = ? AND j2ksec1 = ? AND rid = ?");
			ps.setDouble(1, t0);
			ps.setDouble(2, t1);
			ps.setInt(3, rid);
			rs = ps.executeQuery();
			if (rs.next()) {
				Rank rank;
				int delcount;
				sid			= rs.getInt(1);
				ps			= database.getPreparedStatement("DELETE FROM solutions WHERE sid = ?");
				ps.setInt(1, sid);
				delcount	= ps.executeUpdate();
				ps			= database.getPreparedStatement("DELETE FROM sources WHERE sid = ?");
				ps.setInt(1, sid);
				ps.executeUpdate();
				rank		= defaultGetRank(rid);
				LOGGER.error("deleted j2ksec0:{} / j2ksec1:{} / rank:{} (solutions)", String.valueOf(t0), String.valueOf(t1), rank.getName(), delcount);
			}
			
			// it is now safe to insert this NEW source
			ps = database.getPreparedStatement("INSERT INTO sources (name, j2ksec0, j2ksec1, rid) VALUES (?,?,?,?)");
			ps.setString(1, name);
			ps.setDouble(2, t0);
			ps.setDouble(3, t1);
			ps.setInt(4, rid);
			ps.execute();
			rs = database.getPreparedStatement("SELECT LAST_INSERT_ID()").executeQuery();
			if (rs.next()) {
				sid = rs.getInt(1);
			}
			rs.close();
			
			return sid;
			
		} catch (Exception e) {
			LOGGER.error("SQLGPSDataSource.insertSource() failed.", e);
		}
		
		return sid;
	}
	
	/**
	 * Insert a GPS solution to the database
	 * @param sid	source id
	 * @param cid	channel id
	 * @param dp	data point
	 */
	public void insertSolution(int sid, int cid, DataPoint dp) {
		
		try {
			database.useDatabase(dbName);
			ps = database.getPreparedStatement("INSERT INTO solutions VALUES (?,?,?,?,?,?,?,?,?,?,?)");
			ps.setInt(1, sid);
			ps.setInt(2, cid);
			ps.setDouble(3, dp.x);
			ps.setDouble(4, dp.y);
			ps.setDouble(5, dp.z);
			ps.setDouble(6, dp.sxx);
			ps.setDouble(7, dp.syy);
			ps.setDouble(8, dp.szz);
			ps.setDouble(9, dp.sxy);
			ps.setDouble(10, dp.sxz);
			ps.setDouble(11, dp.syz);
			ps.execute();
			
		} catch (Exception e) {
			LOGGER.error("SQLGPSDataSource.insertSolution() failed.", e);
		}
	}
}
