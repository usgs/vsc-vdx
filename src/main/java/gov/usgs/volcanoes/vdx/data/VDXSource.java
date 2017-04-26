package gov.usgs.volcanoes.vdx.data;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

import gov.usgs.math.DownsamplingType;
import gov.usgs.plot.data.BinaryDataSet;
import gov.usgs.util.ConfigFile;
import gov.usgs.util.Util;
import gov.usgs.volcanoes.core.util.UtilException;
import gov.usgs.volcanoes.vdx.server.BinaryResult;
import gov.usgs.volcanoes.vdx.server.RequestResult;
import gov.usgs.volcanoes.vdx.server.TextResult;
import gov.usgs.volcanoes.winston.Channel;
import gov.usgs.volcanoes.winston.db.Channels;
import gov.usgs.volcanoes.winston.db.Data;
import gov.usgs.volcanoes.winston.db.WinstonDatabase;

/**
 * A base-class for VDX to directly access Winston databases.
 *
 * @author Dan Cervelli
 */
abstract public class VDXSource implements DataSource {
	
	protected WinstonDatabase winston;
	protected Data data;
	protected Channels channels;
	protected String vdxName;
	private int maxrows = 0;
	
	public void initialize(ConfigFile cf) {
		if (winston == null) {
			String driver	= cf.getString("driver");
			String prefix	= cf.getString("prefix");
			String url		= cf.getString("url");
			vdxName			= cf.getName();
			maxrows			= Util.stringToInt(cf.getString("maxrows"), 0); 
			int cacheCap = Util.stringToInt(cf.getString("statementCacheCap"), 100);
			winston			= new WinstonDatabase(driver, url, prefix, cacheCap);
		}
		data = new Data(winston);
		data.setVdxName(vdxName);
		channels = new Channels(winston);
	}
	
	public void defaultDisconnect() {
		winston.close();
	}
	
	public RequestResult getData(Map<String, String> params) {
		
		String action = params.get("action");
		
		// don't initially check that the action is not null, because not all requests define this
		if (action.equals("channels")) {
			List<Channel> chs	= channels.getChannels();
			List<String> result	= new ArrayList<String>();
			for (Channel ch : chs)
				result.add(ch.toVDXString());
			return new TextResult(result);
			
		} else if (action.equals("data") || action == null) {
			int cid			= Integer.parseInt(params.get("ch"));
			double st		= Double.parseDouble(params.get("st"));
			double et		= Double.parseDouble(params.get("et"));
			DownsamplingType ds = DownsamplingType.NONE;
			int dsInt		= 0;
			try{
				ds = DownsamplingType.fromString(params.get("ds"));
				dsInt		= Integer.parseInt(params.get("dsInt")); 
			} catch (Exception e){
				//do nothing
			}
			String code		= channels.getChannelCode(cid);
			BinaryDataSet data = null;
			try{
				data = getData(code, st, et, getMaxRows(), ds, dsInt);
			} catch (UtilException e){
				return getErrorResult(e.getMessage());
			}	
			if (data != null) {
				return new BinaryResult(data);
			}

		} else if (action.equals("suppdata")) {
			double st, et;
			String arg = null;
			List<SuppDatum> data = null;
			SuppDatum sd_s;
			
			String tz = params.get("tz");
			if ( tz==null || tz.equals("") )
				tz = "UTC";
			SimpleDateFormat df = new SimpleDateFormat("yyyyMMddHHmmssSSS");
			df.setTimeZone(TimeZone.getTimeZone(tz));
			
			try {
				arg = params.get("st");
				st =  Util.dateToJ2K(df.parse(arg));
				arg = params.get("et");
				if ( arg==null || arg.equals("") )
					et = Double.MAX_VALUE;
				else
					et = Util.dateToJ2K(df.parse(arg));
			} catch (Exception e) {
				return getErrorResult("Illegal time string: " + arg + ", " + e);
			}
	
			arg = params.get("byID");
			if ( arg != null && arg.equals("true") ) {
				sd_s = new SuppDatum( st, et, -1, -1, -1, -1 );
				String[] args = {"ch","col","rk","type"};
				for ( int i = 0; i<4; i++ ) {
					arg = params.get( args[i] );
					if ( arg==null || arg.equals("") )
						args[i] = null;
					else 
						args[i] = arg;
				}
				sd_s.chName = args[0];
				if ( sd_s.chName != null )
					sd_s.cid = 0;
				sd_s.colName = args[1];
				if ( sd_s.colName != null )
					sd_s.colid = 0;
				sd_s.rkName = args[2];
				if ( sd_s.rkName != null )
					sd_s.rid = 0;
				sd_s.typeName = args[3];
				if ( sd_s.typeName != null )
					sd_s.tid = 0;
			} else {
				String chName   = params.get("ch");
				String colName  = params.get("col");
				String rkName   = params.get("rk");
				String typeName = params.get("type");
				sd_s = new SuppDatum( st, et, chName, colName, rkName, typeName );
			}
			arg = params.get("dl");
			if ( arg != null )
				sd_s.dl = Integer.parseInt(arg);
			try {
				data = getMatchingSuppData( sd_s, false );
			} catch (Exception e) {
				return getErrorResult(e.getMessage());
			}
			if (data != null) {
				List<String> result = new ArrayList<String>();
				for ( SuppDatum sd: data )
					result.add(String.format("%d,%1.3f,%1.3f,%d,%d,%d,%d,%d,\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\"", 
						sd.sdid, sd.st, sd.et, sd.cid, sd.tid, sd.colid, sd.rid, sd.dl, 
						sd.name, sd.value.replace('\n',Character.MIN_VALUE), sd.chName, sd.typeName, sd.colName, sd.rkName, sd.color ));
				return new TextResult(result);
			}
			return null;		
		} else if (action.equals("metadata")) {
			String arg = params.get("byID");
			List<MetaDatum> data = null;
			MetaDatum md_s;
			if ( arg != null && arg.equals("true") ) {
				int cid			= Integer.parseInt(params.get("ch"));
				md_s = new MetaDatum( cid, -1, -1 );
			} else {
				String chName   = params.get("ch");
				md_s = new MetaDatum( chName, null, null );
			}
			
			try{
				data = getMatchingMetaData( md_s, false );
			} catch (Exception e){
				return getErrorResult(e.getMessage());
			}	
			if (data != null) {
				List<String> result = new ArrayList<String>();
				for ( MetaDatum md: data )
					result.add( md.cmid + "," + md.cid + ",\"" + md.name + "\",\"" + md.value + "\",\"" + md.chName + "\"");
				return new TextResult(result);
			}		
		} else if (action.equals("supptypes")) {
			try {
				return getSuppTypes( true );
			} catch (Exception e) {		
				return getErrorResult(e.getMessage());
			}
		}
		return null;
	}
	
	public int getMaxRows(){
		return maxrows;
	}
	
	protected void setMaxRows(int maxrows){
		this.maxrows = maxrows;
	}
	
	public RequestResult getErrorResult(String errMessage){
		List<String> text = new ArrayList<String>();
		text.add(errMessage);
		TextResult result = new TextResult(text);
		result.setError(true);
		return result;
	}
	
	/**
	 * Retrieve a collection of metadata
	 * 
	 * @param md the pattern to match (integers < 0 & null strings are ignored)
	 * @param cm = "is the name of the columns table coulmns_menu?"
	 * @return List<MetaDatum> the desired metadata (null if an error occurred)
	 */
	private List<MetaDatum> getMatchingMetaData( MetaDatum md, boolean cm ) throws Exception {
		winston.useRootDatabase();
		String sql = "SELECT MD.cmid, MD.sid, -1, -1, MD.name, MD.value, CH.code, \"\", \"\" FROM channelmetadata as MD, channels as CH ";
		String where = "WHERE MD.sid=CH.sid";
		
		if ( md.chName != null )
			where = where + " AND CH.code='" + md.chName + "'";
		else if ( md.cid >= 0 )
			where = where + " AND MD.cid=" + md.cid;
		if ( md.name != null )
			where = where + " AND MD.name=" + md.name;
		if ( md.name != null )
			where = where + " AND MD.value=" + md.value;
			
		PreparedStatement ps = winston.getPreparedStatement( sql + where );
		ResultSet rs	= ps.executeQuery();
		List<MetaDatum> result = new ArrayList<MetaDatum>();
		while (rs.next()) {
			md = new MetaDatum();
			md.cmid    = rs.getInt(1);
			md.cid     = rs.getInt(2);
			md.colid   = rs.getInt(3);
			md.rid     = rs.getInt(4);
			md.name    = rs.getString(5);
			md.value   = rs.getString(6);
			md.chName  = rs.getString(7);
			md.colName = rs.getString(8);
			md.rkName  = rs.getString(9);
			result.add( md );
		}
		rs.close();
		return result;
	}

	/**
	 * Retrieve a collection of supplementary data
	 * 
	 * @param sd the pattern to match (integers < 0 & null strings are ignored)
	 * @param cm = "is the name of the columns table coulmns_menu?"
	 * @return List<SuppDatum> the desired supplementary data (null if an error occurred)
	 */
	private List<SuppDatum> getMatchingSuppData( SuppDatum sd, boolean cm )
	{
		winston.useRootDatabase();
		String sql = "SELECT SD.sdid, -1, SD.st, SD.et, SD.sd_short, SD.sd, CH.code, \"\", \"\", ST.supp_data_type, ST.supp_color, SX.cid, -1, -1, ST.draw_line " +
			"FROM supp_data as SD, channels as CH, supp_data_type as ST, supp_data_xref as SX "; // channelmetadata";
		String where = "WHERE SD.et >= " + sd.st + " AND SD.st <= " + sd.et + " AND SD.sdid=SX.sdid AND SD.sdtypeid=ST.sdtypeid AND SX.cid=CH.sid";
		
		if ( sd.chName != null )
			if ( sd.cid < 0 )
				where = where + " AND CH.code='" + sd.chName + "'";
			else
				where = where + " AND CH.sid IN (" + sd.chName + ")";
		else if ( sd.cid >= 0 )
			where = where + " AND CH.sid=" + sd.cid;

		if ( sd.name != null )
			where = where + " AND SD.sd_short=" + sd.name;
		if ( sd.value != null )
			where = where + " AND SD.sd=" + sd.value;

		String type_filter = null;
		if ( sd.typeName != null )
			if ( sd.typeName.length() == 0 )
				;
			else if ( sd.tid == -1 )
				type_filter = "ST.supp_data_type='" + sd.typeName + "'";
			else
				type_filter = "ST.sdtypeid IN (" + sd.typeName + ")";
		else if ( sd.tid >= 0 )
			type_filter = "SD.sdtypeid=" + sd.tid;

		if ( sd.dl == -1 ) {
			if ( type_filter != null )
				where = where + " AND " + type_filter;
		} else if ( sd.dl < 2 ) {
			if ( type_filter != null )
				where = where + " AND " + type_filter;
			where =  where + " AND ST.dl='" + sd.dl;
		} else if ( type_filter != null ) 
			where = where + " AND (" + type_filter + " OR ST.draw_line='0')";
		else
			where = where + " AND ST.draw_line='0'";
			
			
		PreparedStatement ps = winston.getPreparedStatement( sql + where );
		ResultSet rs;
		List<SuppDatum> result = new ArrayList<SuppDatum>();

		try {
			rs = ps.executeQuery();
			while (rs.next()) {
				result.add( new SuppDatum(rs) );
			}
			rs.close();
		} catch (SQLException e) {
		}
		
		return result;
	}
	
	/**
	 * Insert a piece of metadata
	 * 
	 * @param md the MetaDatum to be added
	 */
	public void insertMetaDatum( MetaDatum md ) throws Exception {
		winston.useRootDatabase();

		String sql = "INSERT INTO channelmetadata (sid,name,value) VALUES (" + md.cid + ",\"" + md.name + "\",\"" + md.value + "\");";

		PreparedStatement ps = winston.getPreparedStatement(sql);
		
		ps.execute();
	}
	
	/**
	 * Update a piece of metadata
	 * 
	 * @param md the MetaDatum to be updated
	 */
	public void updateMetaDatum( MetaDatum md ) throws Exception {
		winston.useRootDatabase();

		String sql		= "UPDATE channelmetadata SET sid='" + md.cid + 
			"', name='" + md.name + "', value='" + md.value +
			"' WHERE cmid='" + md.cmid + "'";

		PreparedStatement ps = winston.getPreparedStatement(sql);
		
		ps.execute();
	}
	
	public Channels getChannels() {
		return channels;
	}

	/**
	 * Retrieve the collection of supplementary data types
	 * 
	 * @return List<SuppDatum> the desired supplementary data types
	 */
	public List<SuppDatum> getSuppDataTypes() throws Exception {

		List<SuppDatum> types = new ArrayList<SuppDatum>();
		winston.useRootDatabase();
		String sql  = "SELECT * FROM supp_data_type ORDER BY supp_data_type";
		PreparedStatement ps = winston.getPreparedStatement(sql);
		ResultSet rs = ps.executeQuery();
		while (rs.next()) {
			SuppDatum sd = new SuppDatum( 0.0, 0.0, -1, -1, -1, rs.getInt(1) );
			sd.typeName  = rs.getString(2);
			sd.color     = rs.getString(3);
			sd.dl        = rs.getInt(4);
			types.add(sd);
		}
		rs.close();
		return types;
	}
	
	/**
	 * Insert a piece of supplemental data
	 * 
	 * @param sd the SuppDatum to be added
	 *
	public void insertSuppDatum( SuppDatum sd ) throws Exception {
		winston.useRootDatabase();

		String sql		= "INSERT INTO supp_data (sdid,sdtypeid,st,et,sd_short,sd) VALUES (" + 
			sd.sdid + "," + sd.tid + "," + sd.st + ",\"" + sd.et + "\",\"" + sd.name  + "\",\"" + sd.value + "\");";

		PreparedStatement ps = winston.getPreparedStatement(sql);
		
		ps.execute();
	}*/
	/**
	 * Insert a piece of supplemental data
	 * 
	 * @param sd the SuppDatum to be added
	 */
	public int insertSuppDatum( SuppDatum sd ) throws Exception {
		String sql;
		PreparedStatement ps;
		ResultSet rs;
		
		try {
			winston.useRootDatabase();

			sql		= "INSERT INTO supp_data (sdtypeid,st,et,sd_short,sd) VALUES (" + 
				sd.tid + "," + sd.st + "," + sd.et + ",\"" + sd.name  + "\",\"" + sd.value + 
				"\")";

			ps = winston.getPreparedStatement(sql);
			
			ps.execute();
			
			rs = ps.getGeneratedKeys(); 

			rs.next();
			
			return rs.getInt(1); 
		} catch (SQLException e) {
			if ( !e.getSQLState().equals("23000") ) {
				throw e;
			}
		}
		sql = "SELECT sdid FROM supp_data WHERE sdtypeid=" + sd.tid + " AND st=" + sd.st +
			" AND et=" + sd.et + " AND sd_short='" + sd.name + "'";
		ps = winston.getPreparedStatement(sql);
		
//		int sdid = 0;

		rs = ps.executeQuery();

		rs.next();

		return -rs.getInt(1);
	}

	/**
	 * Update a piece of supplemental data
	 * 
	 * @param sd the SuppDatum to be added
	 * @return ID of the record, 0 if failed
	 */
	public int updateSuppDatum( SuppDatum sd ) throws Exception {
		String sql;
		PreparedStatement ps;
//		ResultSet rs;
		winston.useRootDatabase();

		sql		= "UPDATE supp_data SET sdtypeid='" + sd.tid + "',st='" + sd.st + "',et='" + sd.et + 
			"',sd_short='" + sd.name + "',sd='" + sd.value + "' WHERE sdid='" + sd.sdid + "'";
			
		ps = winston.getPreparedStatement(sql);
		
		ps.execute();
		
		return sd.sdid;
	}	
								
	/**
	 * Insert a supplemental data xref
	 * 
	 * @param sd the SuppDatum xref to be added
	 * @return insertion was successful
	 */
	public boolean insertSuppDatumXref( SuppDatum sd ) throws Exception {
		String sql;
		PreparedStatement ps;
//		ResultSet rs;
		try {
			winston.useRootDatabase();

			sql		= "INSERT INTO supp_data_xref (sdid, cid) VALUES (" +
				sd.sdid + "," + sd.cid + ");";

			ps = winston.getPreparedStatement(sql);

			ps.execute();
		} catch (SQLException e) {
			if ( !e.getSQLState().equals("23000") ) {
				throw e;
			}
			return false;
		}
		return true;
	}
	
	/**
	 * Insert a supplemental datatype
	 * 
	 * @param sd the datatype to be added
	 * @return ID of the datatype, -ID if already present, 0 if failed
	 */
	public int insertSuppDataType( SuppDatum sd ) throws Exception {
		String sql;
		PreparedStatement ps;
		ResultSet rs;
		try {
			sql		= "INSERT INTO supp_data_type (supp_data_type,supp_color,draw_line) VALUES (" + 
				"\"" + sd.typeName + "\",\"" + sd.color + "\"," + sd.dl + ");";

			ps = winston.getPreparedStatement(sql);
			
			ps.execute();
			
			rs = ps.getGeneratedKeys(); 

			rs.next();
			
			return rs.getInt(1); 
		} catch (SQLException e) {
			if ( !e.getSQLState().equals("23000") ) {
				throw e;
			}
		}
		sql = "SELECT sdid FROM supp_data WHERE sdtypeid=" + sd.tid + " AND st=" + sd.st +
			" AND et=" + sd.et + " AND sd_short='" + sd.name + "'";
		ps = winston.getPreparedStatement(sql);
		rs = ps.executeQuery();
		rs.next();
		return -rs.getInt(1);
	}
	
	/**
	 * Process a getData request for supplementary data from this datasource
	 * 
	 * @param params parameters for this request
	 * @param cm = "is the name of the columns table coulmns_menu?"
	 * @return RequestResult the desired supplementary data (null if an error occurred)
	 */

	protected RequestResult getSuppData(Map<String, String> params, boolean cm)
	{
		double st, et;
		String arg = null;
		List<SuppDatum> data = null;
		SuppDatum sd_s;
		
		String tz = params.get("tz");
		if ( tz==null || tz=="" )
			tz = "UTC";
		SimpleDateFormat df = new SimpleDateFormat("yyyyMMddHHmmssSSS");
		df.setTimeZone(TimeZone.getTimeZone(tz));
		
		try {
			arg = params.get("st");
			st =  Util.dateToJ2K(df.parse(arg));
			arg = params.get("et");
			if ( arg==null || arg=="" )
				et = Double.MAX_VALUE;
			else
				et = Util.dateToJ2K(df.parse(arg));
		} catch (Exception e) {
			return getErrorResult("Illegal time string: " + arg + ", " + e);
		}

		arg = params.get("byID");
		if ( arg != null && arg.equals("true") ) {
			int cid			= Integer.parseInt(params.get("ch"));
			arg = params.get("et");
			if ( arg==null || arg=="" )
				et = Double.MAX_VALUE;
			else
				et = Double.parseDouble(arg);
			arg = params.get("type");
			int tid;
			if ( arg==null || arg=="" )
				tid = -1;
			else
				tid = Integer.parseInt(arg);
			sd_s = new SuppDatum( st, et, cid, -1, -1, tid );
		} else {
			String chName   = params.get("ch");
			String typeName = params.get("type");
			sd_s = new SuppDatum( st, et, chName, null, null, typeName );
		}
		data = getMatchingSuppData( sd_s, cm );
		if (data != null) {
			List<String> result = new ArrayList<String>();
			for ( SuppDatum sd: data )
				result.add(String.format("%d,%1.3f,%1.3f,%d,%d,%d,%d,%d,\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\"", 
					sd.sdid, sd.st, sd.et, sd.cid, sd.tid, sd.colid, sd.rid, sd.dl, 
					sd.name, sd.value.replace('\n',Character.MIN_VALUE), sd.chName, sd.typeName, sd.colName, sd.rkName, sd.color ));
			return new TextResult(result);
		}
		return null;
	}
	
	/**
	 * Get supp data types list in format 'sdtypeid"draw_line"name"color' from database
	 * 
	 * @param drawOnly ="yield only types that can be drawn"
	 * @return List of Strings with " separated values
	 */
	public RequestResult getSuppTypes(boolean drawOnly) throws Exception {
		winston.useRootDatabase();
		List<String> result = new ArrayList<String>();

		String sql;
		//PreparedStatement ps;
		ResultSet rs;
		sql = "SELECT * FROM supp_data_type";
		if ( drawOnly )
			sql = sql + " WHERE draw_line=1";
		rs = winston.getPreparedStatement(sql + " ORDER BY supp_data_type").executeQuery();
		while (rs.next()) {
			result.add(String.format("%d\"%d\"%s\"%s", rs.getInt(1), rs.getInt(4), rs.getString(2), rs.getString(3)));
		}
		rs.close();

		return new TextResult(result);
	}
	
	
	abstract public void disconnect();
	abstract protected BinaryDataSet getData(String channel, double st, double et, int maxrows, DownsamplingType ds, int dsInt) throws UtilException;
}
