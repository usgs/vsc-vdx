package gov.usgs.volcanoes.vdx.data.hypo;

import gov.usgs.volcanoes.core.configfile.ConfigFile;
import gov.usgs.volcanoes.core.util.UtilException;
import gov.usgs.volcanoes.vdx.data.DataSource;
import gov.usgs.volcanoes.vdx.data.SelectOption;
import gov.usgs.volcanoes.vdx.data.SQLDataSource;
import gov.usgs.volcanoes.vdx.server.BinaryResult;
import gov.usgs.volcanoes.vdx.server.RequestResult;
import gov.usgs.volcanoes.vdx.server.TextResult;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * SQL Data Source for Hypocenter Data
 *
 * @author Dan Cervelli, Loren Antolik
 */
public class SQLHypocenterDataSource extends SQLDataSource implements DataSource {

	private static final Logger LOGGER = LoggerFactory.getLogger(SQLHypocenterDataSource.class);

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
			
			LOGGER.info("SQLHypocenterDataSource.createDatabase({}_{}) succeeded.",
					database.getDatabasePrefix(), dbName);
			
			return true;
			
		} catch (Exception e) {
			LOGGER.error("SQLHypocenterDataSource.createDatabase({}_{}) failed.",
					database.getDatabasePrefix(), dbName, e);
		}
		return false;
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
			double minStDst		= Double.parseDouble(params.get("minStDst"));
			double maxStDst		= Double.parseDouble(params.get("maxStDst"));
			double maxGap		= Double.parseDouble(params.get("maxGap"));
			double centerLat    = Double.parseDouble(params.get("centerLat"));
			double centerLon    = Double.parseDouble(params.get("centerLon"));
			double radius       = Double.parseDouble(params.get("radius"));
			HypocenterList data = null;
			try{
				data = getHypocenterData(rid, st, et, west, east, south, north, minDepth, maxDepth, minMag, maxMag,
					minNPhases, maxNPhases, minRMS, maxRMS, minHerr, maxHerr, minVerr, maxVerr, rmk, minStDst,
					maxStDst, maxGap, centerLat, centerLon, radius, getMaxRows());
			} catch (UtilException e){
				return getErrorResult(e.getMessage());
			}
			if (data != null)
				return new BinaryResult(data);
		} else if (action.equals("metadata")) {
			return getMetaData( params, false );
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
	 * @param maxMag		maximum magnitude
	 * @param minNPhases	minimum number of phases
	 * @param maxNPhases	maximum number of phases
	 * @param minRMS		minimum RMS
	 * @param maxRMS		maximum RMS
	 * @param minHerr		minimum horizontal error
	 * @param maxHerr		maximum horizontal error
	 * @param minVerr		minimum vertical error
	 * @param maxVerr		maximum vertical error
	 * @param rmk			remarks filter
	 * @param minStDst		min distance from closest station
	 * @param maxStDst		max distance from closest station
	 * @param maxGap		max gap filter
	 * @param centerLat     latitude of the center point
	 * @param centerLon     longitude of the center point
	 * @param radius        radius to search within
	 * @param maxrows       maximum nbr of rows returned
	 * @return list of hypocenter data
	 * @throws UtilException
	 */
	public HypocenterList getHypocenterData(int rid, double st, double et, double west, double east, 
			double south, double north, double minDepth, double maxDepth, double minMag, double maxMag,
			Integer minNPhases, Integer maxNPhases, double minRMS, double maxRMS, 
			double minHerr, double maxHerr, double minVerr, double maxVerr, String rmk, double minStDst, 
			double maxStDst, double maxGap, double centerLat, double centerLon, double radius, 
			int maxrows) throws UtilException {

		List<Hypocenter> pts	= new ArrayList<Hypocenter>();
		HypocenterList result	= null;
		
		if (radius > 0.0) {
		    getHypocenterData(rid, st, et, minDepth, maxDepth, minMag, maxMag, minNPhases, maxNPhases, 
		            minRMS, maxRMS, minHerr, maxHerr, minVerr, maxVerr, rmk, minStDst, maxStDst, maxGap, 
		            centerLat, centerLon, radius, maxrows);
		}
		else {
		    getHypocenterData(rid, st, et, west, east, south, north, minDepth, maxDepth, minMag, 
		            maxMag, minNPhases, maxNPhases, minRMS, maxRMS, minHerr, maxHerr, minVerr, 
		            maxVerr, rmk, minStDst, maxStDst, maxGap, maxrows);
		}
    			
		double j2ksec, lat, lon, depth, mag;
		double ampmag, codamag, dmin, rms, herr, verr;
		int nphases, azgap, nstimes;
		String magtype, rs_rmk;
		String tempEid = "";
		
		try {
    		// loop through each result and add to the list
    		while (rs.next()) {
    			
    			// if this is a new eid, then save this data, as it contains the highest rank
    			if (!tempEid.equals(rs.getString(18))) {
    				
    				// these will never be null
    				j2ksec	= getDoubleNullCheck(rs, 1);
    				rid		= rs.getInt(2);
    				lat		= getDoubleNullCheck(rs, 3);
    				lon		= getDoubleNullCheck(rs, 4);
    				depth	= getDoubleNullCheck(rs, 5);
    				mag		= getDoubleNullCheck(rs, 6);
    
    				ampmag	= getDoubleNullCheck(rs, 7);
    				codamag	= getDoubleNullCheck(rs, 8);
    				nphases	= getIntNullCheck(rs, 9);
    				azgap	= getIntNullCheck(rs, 10);
    				dmin	= getDoubleNullCheck(rs, 11);
    				rms		= getDoubleNullCheck(rs, 12);
    				nstimes	= getIntNullCheck(rs, 13);
    				herr	= getDoubleNullCheck(rs, 14);
    				verr	= getDoubleNullCheck(rs, 15);
    				magtype	= rs.getString(16);
    				rs_rmk	= rs.getString(17);
    				
    				pts.add(new Hypocenter(j2ksec, (String)null, rid, lat, lon, depth, mag,
    						ampmag, codamag, nphases, azgap, dmin, rms, nstimes,
    						herr, verr, magtype, rs_rmk ));
    			}
    			tempEid = rs.getString(18);
    		}
    		rs.close();
        			
    		if (pts.size() > 0) {
    			Collections.sort(pts, new Comparator<Hypocenter>() {
    				public int compare(Hypocenter h1, Hypocenter h2) {
    					if (h1.j2ksec > h2.j2ksec) {
    						return 1;
    					} else {
    						return -1;
    					}
    				}
    			});
    		}
    		result = new HypocenterList(pts);
		} catch (SQLException e) {
            LOGGER.error("SQLHypocenterDataSource.getHypocenterData() failed.", e);
        }
    		
    	return new HypocenterList(pts);
	}

	/**
     * Get Hypocenter data -- boundary version
     * 
     * @param rid           rank id
     * @param st            start time
     * @param et            end time
     * @param west          west boundary
     * @param east          east boundary
     * @param south         south boundary
     * @param north         north boundary
     * @param minDepth      minimum depth
     * @param maxDepth      maximum depth
     * @param minMag        minimum magnitude
     * @param maxMag        maximum magnitude
     * @param minNPhases    minimum number of phases
     * @param maxNPhases    maximum number of phases
     * @param minRMS        minimum RMS
     * @param maxRMS        maximum RMS
     * @param minHerr       minimum horizontal error
     * @param maxHerr       maximum horizontal error
     * @param minVerr       minimum vertical error
     * @param maxVerr       maximum vertical error
     * @param rmk           remarks filter
     * @param minStDst      min distance from closest station
     * @param maxStDst      max distance from closest station
     * @param maxGap        max gap filter
     * @param maxrows       maximum nbr of rows returned
     * @return list of hypocenter data
     * @throws UtilException
     */
    public void getHypocenterData(int rid, double st, double et, double west, double east, 
            double south, double north, double minDepth, double maxDepth, double minMag, 
            double maxMag, Integer minNPhases, Integer maxNPhases, double minRMS, 
            double maxRMS, double minHerr, double maxHerr, double minVerr, double maxVerr, 
            String rmk, double minStDst, double maxStDst, double maxGap, int maxrows) throws UtilException {
                
        try {
            
            database.useDatabase(dbName);
            
            // calculate the num of rows to limit the query to
            int tempmaxrows;
            if (rid != 0) {
                tempmaxrows = maxrows;
            } else {
                tempmaxrows = maxrows * defaultGetNumberOfRanks();
            }
            
            sqlCount = "SELECT COUNT(*) FROM (SELECT 1 ";
            
            // build the sql
            sql  = "SELECT a.j2ksec, a.rid, a.lat, a.lon, a.depth, a.prefmag, ";
            sql += "       a.ampmag, a.codamag, a.nphases, a.azgap, a.dmin, a.rms, ";
            sql += "       a.nstimes, a.herr, a.verr, a.magtype, a.rmk, a.eid ";
            sql += "FROM   hypocenters a, ranks c ";
            sql += "WHERE  a.rid = c.rid ";
            sql += "AND    a.j2ksec  >= ? AND a.j2ksec  <= ? ";
            
            if (west <= east)
                sql += "AND a.lon >= ? AND a.lon <= ? ";

            // wrap around date line
            else
                sql += "AND (a.lon >= ? OR a.lon <= ?) ";
                
            sql += "AND    a.lat     >= ? AND a.lat     <= ? ";
            sql += "AND    a.depth   >= ? AND a.depth   <= ? ";
            sql += "AND    a.prefmag >= ? AND a.prefmag <= ? ";
            sql += "AND    a.nphases >= ? AND a.nphases <= ? ";
            sql += "AND    a.rms     >= ? AND a.rms     <= ? ";
            sql += "AND    a.herr    >= ? AND a.herr    <= ? ";
            sql += "AND    a.verr    >= ? AND a.verr    <= ? ";
            sql += "AND    a.dmin    >= ? AND a.dmin    <= ? ";
            sql += "AND    a.azgap   <= ? ";
            sql += "AND    a.prefmag IS NOT NULL ";
            
            // remarks filtering options
            if (!rmk.equals("")) {
                sql += "AND    a.rmk = '" + rmk + "' ";
            }
            
            // BEST AVAILABLE DATA query
            if (ranks && rid != 0) {
                sql += "AND    c.rid  = ? ";
            }
            
            sql += "ORDER BY eid ASC";
            
            if (ranks && rid == 0) {
                sql = sql + ", c.rank DESC";
            }
            
            if (maxrows != 0) {
                sql += " LIMIT " + (tempmaxrows + 1);
                
                // If the dataset has a maxrows paramater, check that the number of requested rows doesn't
                // exceed that number prior to running the full query. This can save a decent amount of time
                // for large queries.
                ps = database.getPreparedStatement(sqlCount + sql.substring(sql.indexOf("FROM")) + ") as T");
                ps.setDouble(1, st);
                ps.setDouble(2, et);
                ps.setDouble(3, west);
                ps.setDouble(4, east);
                ps.setDouble(5, south);
                ps.setDouble(6, north);
                ps.setDouble(7, minDepth);
                ps.setDouble(8, maxDepth);
                ps.setDouble(9, minMag);
                ps.setDouble(10, maxMag);
                ps.setInt(11, minNPhases);
                ps.setInt(12, maxNPhases);
                ps.setDouble(13, minRMS);
                ps.setDouble(14, maxRMS);
                ps.setDouble(15, minHerr);
                ps.setDouble(16, maxHerr);
                ps.setDouble(17, minVerr);
                ps.setDouble(18, maxVerr);
                ps.setDouble(19, minStDst);
                ps.setDouble(20, maxStDst);
                ps.setDouble(21, maxGap);
                if (ranks && rid != 0) {
                    ps.setInt(22, rid);
                }
                rs = ps.executeQuery();
                if (rs.next() && rs.getInt(1) > tempmaxrows)
                    throw new UtilException("Max rows (" + maxrows + " rows) for data source '" + vdxName + "' exceeded.");
            }
            
            ps = database.getPreparedStatement(sql);
            ps.setDouble(1, st);
            ps.setDouble(2, et);
            ps.setDouble(3, west);
            ps.setDouble(4, east);
            ps.setDouble(5, south);
            ps.setDouble(6, north);
            ps.setDouble(7, minDepth);
            ps.setDouble(8, maxDepth);
            ps.setDouble(9, minMag);
            ps.setDouble(10, maxMag);
            ps.setInt(11, minNPhases);
            ps.setInt(12, maxNPhases);
            ps.setDouble(13, minRMS);
            ps.setDouble(14, maxRMS);
            ps.setDouble(15, minHerr);
            ps.setDouble(16, maxHerr);
            ps.setDouble(17, minVerr);
            ps.setDouble(18, maxVerr);
            ps.setDouble(19, minStDst);
            ps.setDouble(20, maxStDst);
            ps.setDouble(21, maxGap);
            if (ranks && rid != 0) {
                ps.setInt(22, rid);
            }
            rs = ps.executeQuery();
            
        } catch (SQLException e) {
            LOGGER.error("SQLHypocenterDataSource.getHypocenterData() failed.", e);
        }
    }
	
    /**
     * Get Hypocenter data based on a center point and radius.
     * Solution taken from: https://developers.google.com/maps/articles/phpsqlsearch_v3#findnearsql
     * 
     * @param rid           rank id
     * @param st            start time
     * @param et            end time
     * @param minDepth      minimum depth
     * @param maxDepth      maximum depth
     * @param minMag        minimum magnitude
     * @param maxMag        maximum magnitude
     * @param minNPhases    minimum number of phases
     * @param maxNPhases    maximum number of phases
     * @param minRMS        minimum RMS
     * @param maxRMS        maximum RMS
     * @param minHerr       minimum horizontal error
     * @param maxHerr       maximum horizontal error
     * @param minVerr       minimum vertical error
     * @param maxVerr       maximum vertical error
     * @param rmk           remarks filter
     * @param minStDst      min distance from closest station
     * @param maxStDst      max distance from closest station
     * @param maxGap        max gap filter
     * @param centerLat     latitude of center point
     * @param centerLon     longitude of center point
     * @param radius        radius to search within
     * @param maxrows       maximum nbr of rows returned
     * @return list of hypocenter data
     * @throws UtilException
     */
	public void getHypocenterData(int rid, double st, double et, double minDepth, 
	        double maxDepth, double minMag, double maxMag, Integer minNPhases, Integer maxNPhases, 
	        double minRMS, double maxRMS, double minHerr, double maxHerr, double minVerr, 
	        double maxVerr, String rmk, double minStDst, double maxStDst, double maxGap, 
	        double centerLat, double centerLon, double radius, int maxrows) throws UtilException {
        
	    try {
            
            database.useDatabase(dbName);
            
            // calculate the num of rows to limit the query to
            int tempmaxrows;
            if (rid != 0) {
                tempmaxrows = maxrows;
            } else {
                tempmaxrows = maxrows * defaultGetNumberOfRanks();
            }
            
            sqlCount = "SELECT COUNT(*) FROM (SELECT 1 ";
            
            // build the sql
            sql  = "SELECT a.j2ksec, a.rid, a.lat, a.lon, a.depth, a.prefmag, ";
            sql += "       a.ampmag, a.codamag, a.nphases, a.azgap, a.dmin, a.rms, ";
            sql += "       a.nstimes, a.herr, a.verr, a.magtype, a.rmk, a.eid, ";
            sql += "       (6371 * acos ( ";
            sql += "            cos(radians(?))";
            sql += "            * cos(radians(a.lat))";
            sql += "            * cos(radians(a.lon) - radians(?))";
            sql += "            + sin(radians(?))";
            sql += "            * sin(radians(a.lat))";
            sql += "       )) AS distance ";
            sql += "FROM   hypocenters a, ranks c ";
            sql += "WHERE  a.rid = c.rid ";
            sql += "AND    a.j2ksec  >= ? AND a.j2ksec  <= ? ";
            sql += "AND    a.depth   >= ? AND a.depth   <= ? ";
            sql += "AND    a.prefmag >= ? AND a.prefmag <= ? ";
            sql += "AND    a.nphases >= ? AND a.nphases <= ? ";
            sql += "AND    a.rms     >= ? AND a.rms     <= ? ";
            sql += "AND    a.herr    >= ? AND a.herr    <= ? ";
            sql += "AND    a.verr    >= ? AND a.verr    <= ? ";
            sql += "AND    a.dmin    >= ? AND a.dmin    <= ? ";
            sql += "AND    a.azgap   <= ? ";
            sql += "AND    a.prefmag IS NOT NULL ";
            
            // remarks filtering options
            if (!rmk.equals("")) {
                sql += "AND    a.rmk = '" + rmk + "' ";
            }
            
            // BEST AVAILABLE DATA query
            if (ranks && rid != 0) {
                sql += "AND    c.rid  = ? ";
            }
            
            sql += "HAVING distance <= ? ";
            sql += "ORDER BY eid ASC";
            
            if (ranks && rid == 0) {
                sql = sql + ", c.rank DESC";
            }
            
            if (maxrows != 0) {
                sql += " LIMIT " + (tempmaxrows + 1);
                
                // If the dataset has a maxrows paramater, check that the number of requested rows doesn't
                // exceed that number prior to running the full query. This can save a decent amount of time
                // for large queries.
                ps = database.getPreparedStatement(sqlCount + sql.substring(sql.indexOf("FROM")) + ") as T");
                ps.setDouble(1, centerLat);
                ps.setDouble(2, centerLon);
                ps.setDouble(3, centerLat);
                ps.setDouble(4, st);
                ps.setDouble(5, et);
                ps.setDouble(6, minDepth);
                ps.setDouble(7, maxDepth);
                ps.setDouble(8, minMag);
                ps.setDouble(9, maxMag);
                ps.setInt(10, minNPhases);
                ps.setInt(11, maxNPhases);
                ps.setDouble(12, minRMS);
                ps.setDouble(13, maxRMS);
                ps.setDouble(14, minHerr);
                ps.setDouble(15, maxHerr);
                ps.setDouble(16, minVerr);
                ps.setDouble(17, maxVerr);
                ps.setDouble(18, minStDst);
                ps.setDouble(19, maxStDst);
                ps.setDouble(20, maxGap);
                if (ranks && rid != 0) {
                    ps.setInt(21, rid);
                    ps.setDouble(22, radius);
                } else {
                    ps.setDouble(21, radius);
                }
                rs = ps.executeQuery();
                if (rs.next() && rs.getInt(1) > tempmaxrows)
                    throw new UtilException("Max rows (" + maxrows + " rows) for data source '" + vdxName + "' exceeded.");
            }
            
            ps = database.getPreparedStatement(sql);
            ps.setDouble(1, centerLat);
            ps.setDouble(2, centerLon);
            ps.setDouble(3, centerLat);
            ps.setDouble(4, st);
            ps.setDouble(5, et);
            ps.setDouble(6, minDepth);
            ps.setDouble(7, maxDepth);
            ps.setDouble(8, minMag);
            ps.setDouble(9, maxMag);
            ps.setInt(10, minNPhases);
            ps.setInt(11, maxNPhases);
            ps.setDouble(12, minRMS);
            ps.setDouble(13, maxRMS);
            ps.setDouble(14, minHerr);
            ps.setDouble(15, maxHerr);
            ps.setDouble(16, minVerr);
            ps.setDouble(17, maxVerr);
            ps.setDouble(18, minStDst);
            ps.setDouble(19, maxStDst);
            ps.setDouble(20, maxGap);
            if (ranks && rid != 0) {
                ps.setInt(21, rid);
                ps.setDouble(22, radius);
            } else {
                ps.setDouble(21, radius);
            }
            rs = ps.executeQuery();
            
        } catch (SQLException e) {
            LOGGER.error("SQLHypocenterDataSource.getHypocenterData() failed.", e);
        }
	}
	
	/**
	 * Insert data
	 * @param hc	Hypocenter
	 */
	public int insertHypocenter(Hypocenter hc) {
		
		int result = -1;
		
		try {
			database.useDatabase(dbName);
			sql = "REPLACE INTO hypocenters ";
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
			
			result = ps.executeUpdate();
			
		} catch (Exception e) {
			LOGGER.error("SQLHypocenterDataSource.insertHypocenter() failed.", e);
		}
		return result;
	}
}
