package gov.usgs.vdx.data.hypo;

import gov.usgs.util.ConfigFile;
import gov.usgs.util.UtilException;
import gov.usgs.vdx.data.DataSource;
import gov.usgs.vdx.data.SelectOption;
import gov.usgs.vdx.data.SQLDataSource;
import gov.usgs.vdx.server.BinaryResult;
import gov.usgs.vdx.server.RequestResult;
import gov.usgs.vdx.server.TextResult;

import java.sql.SQLException;
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
	public static final boolean columns			= false;
	public static final boolean menuColumns		= false;

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
	
	public static final String REMARK	= "remark";
	public static final String MAGTYPE	= "magtype";
	
	public static final SelectOption[] REMARK_OPTIONS	= new SelectOption[] {
		new SelectOption(1, "F", "Felt (F)"),
		new SelectOption(2, "B", "Explosion/Collapse (B)"),
		new SelectOption(3, "L", "Long Period (L)"),
		new SelectOption(4, "T", "Tremor (T)"),
		new SelectOption(5, "Q", "Quarry (Q)"),
		new SelectOption(6, "N", "Nuclear (N"),};
	
	public static final SelectOption[] MAGTYPE_OPTIONS	= new SelectOption[] {
		new SelectOption(1, "B", "Body Wave Magnitude (B)"),
		new SelectOption(2, "C", "Coda Amplitude Magnitude (C)"),
		new SelectOption(3, "D", "Coda Duration Magnitude (D)"),
		new SelectOption(4, "E", "Energy Magnitude (E)"),
		new SelectOption(5, "G", "MAGNUM (G)"),
		new SelectOption(1, "H", "Hand Entered (H)"),
		new SelectOption(2, "L", "Local Magnitude (L)"),
		new SelectOption(3, "O", "Moment Magnitude (O)"),
		new SelectOption(4, "P", "P-Wave Magnitude (P)"),
		new SelectOption(5, "S", "Surface Wave Magnitude (S)"),
		new SelectOption(6, "X", "External (X)"),};
	
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
	 * Get flag if database exists
	 * @return true if database exists, false otherwise
	 */
	public boolean databaseExists() {
		return defaultDatabaseExists();
	}
	
	/**
	 * Create hypocenters database
	 * @return true if successful, false otherwise
	 */
	public boolean createDatabase() {
		
		try {
			defaultCreateDatabase(channels, translations, channelTypes, ranks, columns, menuColumns);
			
			// setup the database for running some statements
			database.useDatabase(dbName);
			st	= database.getStatement();
			
			// create the hypocenters table
			sql	= "CREATE TABLE hypocenters (j2ksec DOUBLE NOT NULL, eid VARCHAR(45) NOT NULL, rid INT NOT NULL, ";
			sql+= "   lat DOUBLE NOT NULL, lon DOUBLE NOT NULL, depth DOUBLE NOT NULL, ";
			sql+= "   prefmag DOUBLE, ampmag DOUBLE, codamag DOUBLE, ";
			sql+= "   nphases INT, azgap INT, dmin DOUBLE, rms DOUBLE, nstimes INT, herr DOUBLE, verr DOUBLE, ";
			sql+= "   magtype VARCHAR(1), rmk VARCHAR(1), PRIMARY KEY(eid,rid), KEY index_j2ksec (j2ksec))";
			st.execute(sql);
			
			// create the remarks table
			sql	= "CREATE TABLE options_" + REMARK + " (id INT PRIMARY KEY AUTO_INCREMENT, ";
			sql+= "   idx INT NOT NULL, code VARCHAR(1) NOT NULL, name VARCHAR(255) NOT NULL)";
			st.execute(sql);
			
			// create the mag type table
			sql	= "CREATE TABLE options_" + MAGTYPE + " (id INT PRIMARY KEY AUTO_INCREMENT, ";
			sql+= "   idx INT NOT NULL, code VARCHAR(1) NOT NULL, name VARCHAR(255) NOT NULL)";
			st.execute(sql);
			
			SelectOption so;
			
			// populate the remarks table
			for (int i = 0; i < REMARK_OPTIONS.length; i++) {
				so	= REMARK_OPTIONS[i];
				sql = "INSERT INTO options_" + REMARK + " (idx, code, name) VALUES(?,?,?)";
				ps	= database.getPreparedStatement(sql);
				ps.setInt(1, so.getIndex());
				ps.setString(2, so.getCode());
				ps.setString(3, so.getName());
				ps.execute();
			}
			
			// populate the mag type table
			for (int i = 0; i < MAGTYPE_OPTIONS.length; i++) {
				so	= MAGTYPE_OPTIONS[i];
				sql = "INSERT INTO options_" + MAGTYPE + " (idx, code, name) VALUES(?,?,?)";
				ps	= database.getPreparedStatement(sql);
				ps.setInt(1, so.getIndex());
				ps.setString(2, so.getCode());
				ps.setString(3, so.getName());
				ps.execute();
			}
			
			logger.log(Level.INFO, "SQLHypocenterDataSource.createDatabase(" + database.getDatabasePrefix() + "_" + dbName + ") succeeded.");
			
			return true;
			
		} catch (Exception e) {
			logger.log(Level.SEVERE, "SQLHypocenterDataSource.createDatabase(" + database.getDatabasePrefix() + "_" + dbName + ") failed.", e);
		}
		return false;
	}

	/**
	 * Getter for data. 
	 * Search value of 'action' parameter and retrieve corresponding data.
	 * @param command to execute, map of parameter-value pairs.
	 * @return request result
	 */	
	public RequestResult getData(Map<String, String> params) {
		
		String action = params.get("action");
		
		if (action == null) {
			return null;		

		} else if (action.equals("ranks")) {
			return new TextResult(defaultGetRanks());
			
		} else if (action.equals(REMARK) || action.equals(MAGTYPE)) {
			return new TextResult(defaultGetOptions(action));
			
		} else if (action.equals("data")) {
			int rid				= Integer.parseInt(params.get("rk"));
			double st			= Double.parseDouble(params.get("st"));
			double et			= Double.parseDouble(params.get("et"));
			double west			= Double.parseDouble(params.get("west"));
			double east			= Double.parseDouble(params.get("east"));
			double south		= Double.parseDouble(params.get("south"));
			double north		= Double.parseDouble(params.get("north"));
			double minDepth		= Double.parseDouble(params.get("minDepth"));
			double maxDepth		= Double.parseDouble(params.get("maxDepth"));
			double minMag		= Double.parseDouble(params.get("minMag"));
			double maxMag		= Double.parseDouble(params.get("maxMag"));
			Integer minNPhases	= Integer.parseInt(params.get("minNPhases"));
			Integer maxNPhases	= Integer.parseInt(params.get("maxNPhases"));
			double minRMS		= Double.parseDouble(params.get("minRMS"));
			double maxRMS		= Double.parseDouble(params.get("maxRMS"));
			double minHerr		= Double.parseDouble(params.get("minHerr"));
			double maxHerr		= Double.parseDouble(params.get("maxHerr"));
			double minVerr		= Double.parseDouble(params.get("minVerr"));
			double maxVerr		= Double.parseDouble(params.get("maxVerr"));
			String rmk			= params.get("rmk");
			HypocenterList data = null;
			try{
				data = getHypocenterData(rid, st, et, west, east, south, north, minDepth, maxDepth, minMag, maxMag,
					minNPhases, maxNPhases, minRMS, maxRMS, minHerr, maxHerr, minVerr, maxVerr, rmk, getMaxRows());
			} catch (UtilException e){
				return getErrorResult(e.getMessage());
			}
			if (data != null && data.size() > 0)
				return new BinaryResult(data);
		}
		return null;
	}
	
	/**
	 * Get Hypocenter data
	 * 
	 * @param rid			rank id
	 * @param st			start time
	 * @param et			end time
	 * @param west			west boundary
	 * @param east			east boundary
	 * @param south			south boundary
	 * @param north			north boundary
	 * @param minDepth		minimum depth
	 * @param maxDepth		maximum depth
	 * @param minMag		minimum magnitude
	 * @param maxMax		maximum magnitude
	 * @param minNPhases	minimum number of phases
	 * @param maxNPhases	maximum number of phases
	 * @param minRMS		minimum RMS
	 * @param maxRMS		maximum RMS
	 * @param minHerr		minimum horizontal error
	 * @param maxHerr		maximum horizontal error
	 * @param minVerr		minimum vertical error
	 * @param maxVerr		maximum vertical error
	 * @param rmk			remarks filter
	 * @return list of hypocenter data
	 */
	public HypocenterList getHypocenterData(int rid, double st, double et, double west, double east, 
			double south, double north, double minDepth, double maxDepth, double minMag, double maxMag,
			Integer minNPhases, Integer maxNPhases, double minRMS, double maxRMS, 
			double minHerr, double maxHerr, double minVerr, double maxVerr, String rmk, int maxrows) throws UtilException {

		List<Hypocenter> pts	= new ArrayList<Hypocenter>();
		HypocenterList result	= null;
		
		try {
			database.useDatabase(dbName);
			
			// build the sql
			sql  = "SELECT a.j2ksec, a.rid, a.lat, a.lon, a.depth, a.prefmag ";
			sql += "FROM   hypocenters a, ranks c ";
			sql += "WHERE  a.rid = c.rid ";
			sql += "AND    a.j2ksec  >= ? AND a.j2ksec  <= ? ";
			sql += "AND    a.lon     >= ? AND a.lon     <= ? ";
			sql += "AND    a.lat     >= ? AND a.lat     <= ? ";
			
			// depth filtering options
			if (!(minDepth == -Double.MAX_VALUE && maxDepth == Double.MAX_VALUE)) {
				sql += "AND    a.depth   >= " + minDepth + " AND a.depth   <= " + maxDepth + " ";
			}
			
			// mag filtering options
			if (!(minMag == -Double.MAX_VALUE && maxMag == Double.MAX_VALUE)) {
				sql += "AND    a.prefmag >= " + minMag + " AND a.prefmag <= " + maxMag + " ";
			}
			
			// dont' return values without a magnitude
			sql += "AND    a.prefmag IS NOT NULL ";
			
			// number of phases filtering options
			if (!(minNPhases == Integer.MIN_VALUE && maxNPhases == Integer.MAX_VALUE)) {
				sql += "AND    a.nphases >= " + minNPhases + " AND a.nphases <= " + maxNPhases + " ";
			}
			
			// rms filtering options
			if (!(minRMS == -Double.MAX_VALUE && maxRMS == Double.MAX_VALUE)) {
				sql += "AND    a.rms >= " + minRMS + " AND a.rms <= " + maxRMS + " ";
			}
			
			// herr filtering options
			if (!(minHerr == -Double.MAX_VALUE && maxHerr == Double.MAX_VALUE)) {
				sql += "AND    a.herr >= " + minHerr + " AND a.herr <= " + maxHerr + " ";
			}
			
			// verr filtering options
			if (!(minVerr == -Double.MAX_VALUE && maxVerr == Double.MAX_VALUE)) {
				sql += "AND    a.verr >= " + minVerr + " AND a.verr <= " + maxVerr + " ";
			}
			
			// remarks filtering options
			if (!rmk.equals("")) {
				sql += "AND    a.rmk = '" + rmk + "' ";
			}
			
			// BEST POSSIBLE DATA query
			if (ranks && rid != 0) {
				sql += "AND    c.rid  = ? ";
			} else if (ranks && rid == 0) {
				sql += "AND    c.rank = (SELECT MAX(e.rank) " +
	            					    "FROM   hypocenters d, ranks e " +
	            					    "WHERE  d.rid = e.rid  " +
	            					    "AND    trim(a.eid) = trim(d.eid) " +
	            					    "AND    d.j2ksec >= ? " +
	            					    "AND    d.j2ksec <= ? ) ";
			}
			
			sql += "ORDER BY j2ksec ASC";
			if(maxrows !=0){
				sql += " LIMIT " + (maxrows+1);
			}
			ps = database.getPreparedStatement(sql);
			ps.setDouble(1, st);
			ps.setDouble(2, et);
			ps.setDouble(3, west);
			ps.setDouble(4, east);
			ps.setDouble(5, south);
			ps.setDouble(6, north);
			if (ranks && rid != 0) {
				ps.setInt(7, rid);
			} else {
				ps.setDouble(7, st);
				ps.setDouble(8, et);
			}
			rs = ps.executeQuery();
			if(maxrows !=0 && getResultSetSize(rs)> maxrows){ 
				throw new UtilException("Configured row count (" + maxrows + " rows) for data source '" + vdxName + "' exceeded.");
			}
			
			double j2ksec, lat, lon, depth, mag;
			// these will never be null
			while (rs.next()) {				
				j2ksec	= getDoubleNullCheck(rs, 1);
				rid		= rs.getInt(2);
				lat		= getDoubleNullCheck(rs, 3);
				lon		= getDoubleNullCheck(rs, 4);
				depth	= getDoubleNullCheck(rs, 5);
				mag		= getDoubleNullCheck(rs, 6);
				pts.add(new Hypocenter(j2ksec, rid, lat, lon, depth, mag));
			}
			rs.close();
			
			if (pts.size() > 0) {
				return new HypocenterList(pts);
			}
			
		} catch (SQLException e) {
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
			sql = "INSERT IGNORE INTO hypocenters ";
			sql+= "       (j2ksec, eid, rid, lat, lon, depth, prefmag, ampmag, codamag, ";
			sql+= "        nphases, azgap, dmin, rms, nstimes, herr, verr, magtype, rmk) ";
			sql+= "VALUES (?,?,?,round(?, 4),round(?, 4),round(?, 2),round(?, 2),round(?, 2),round(?, 2),?,?,?,?,?,?,?,?,?)";
			ps = database.getPreparedStatement(sql);
			
			// required fields
			ps.setDouble(1, hc.j2ksec);
			ps.setString(2, hc.eid);
			ps.setInt(3, hc.rid);
			ps.setDouble(4, hc.lat);
			ps.setDouble(5, hc.lon);
			ps.setDouble(6, hc.depth);
			
			// non-required fields
			if (Double.isNaN(hc.prefmag))	ps.setNull(7,  java.sql.Types.DOUBLE);		else ps.setDouble(7, hc.prefmag);
			if (Double.isNaN(hc.ampmag))	ps.setNull(8,  java.sql.Types.DOUBLE);		else ps.setDouble(8, hc.ampmag);
			if (Double.isNaN(hc.codamag))	ps.setNull(9,  java.sql.Types.DOUBLE);		else ps.setDouble(9, hc.codamag);
			if (hc.nphases == null)			ps.setNull(10, java.sql.Types.INTEGER);		else ps.setInt(10, hc.nphases);
			if (hc.azgap == null)			ps.setNull(11, java.sql.Types.INTEGER);		else ps.setInt(11, hc.azgap);
			if (Double.isNaN(hc.dmin))		ps.setNull(12, java.sql.Types.DOUBLE);		else ps.setDouble(12, hc.dmin);
			if (Double.isNaN(hc.rms))		ps.setNull(13, java.sql.Types.DOUBLE);		else ps.setDouble(13, hc.rms);
			if (hc.nstimes == null)			ps.setNull(14, java.sql.Types.INTEGER);		else ps.setInt(14, hc.nstimes);
			if (Double.isNaN(hc.herr))		ps.setNull(15, java.sql.Types.DOUBLE);		else ps.setDouble(15, hc.herr);
			if (Double.isNaN(hc.verr))		ps.setNull(16, java.sql.Types.DOUBLE);		else ps.setDouble(16, hc.verr);
			if (hc.magtype == null)			ps.setNull(17, java.sql.Types.VARCHAR);		else ps.setString(17, hc.magtype);
			if (hc.rmk == null)				ps.setNull(18, java.sql.Types.VARCHAR);		else ps.setString(18, hc.rmk);
			
			ps.execute();
			
		} catch (Exception e) {
			logger.log(Level.SEVERE, "SQLHypocenterDataSource.insertHypocenter() failed.", e);
		}
	}
}
