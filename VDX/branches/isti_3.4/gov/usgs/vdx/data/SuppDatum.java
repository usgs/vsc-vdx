package gov.usgs.vdx.data;
	
import java.lang.Character;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Supplemental data for channel		
 */

public class SuppDatum
{
	public int sdid;
	public int cid;
	public String chName;
	public int colid;
	public String colName;
	public int rid;
	public String rkName;
	public String name;
	public String value;
	public Double st;
	public Double et;
	public int tid;
	public String typeName;
	public String color;
	public int dl;
	
	/**
	 * SuppDatum default constructor
	 */
	public SuppDatum() {
	}

	/**
	 * Yield null if s is an empty string, otherwise just s
	 */
	private String empty2null( String s ) {
		if ( s.length() == 0 )
			return null;
		else
			return s;
	}

	/**
	 * Yield -1 if s is null or an empty string, otherwise the integer s parses to
	 */
	private int str2int( String s ) {
		if ( s==null || s.length() == 0 )
			return -1;
		else
			return Integer.parseInt( s );
	}

	/**
	 * Yield -1 if s is null or an empty string, otherwise the double s parses to
	 */
	private double str2dbl( String s ) {
		if ( s==null || s.length() == 0 )
			return -1;
		else
			return Double.parseDouble( s );
	}


	/**
	 * Constructor
	 *   String rep of SuppDatum is a CSV with:
	 *		- strings each enclosed in quotes
	 *		- all MIN_VALUE characters will be translated to newlines
	 *		- component order: sdid, st, et, cid, tid, colid, rid, name, value, chName, typeName, colName, rkName, color
	 * @param str CSV rep of a SuppDatum
	 */
	public SuppDatum( String str ) {
//		sd.sdid, sd.st, sd.et, sd.cid, sd.tid, sd.colid, sd.rid, sd.dl, 
//		sd.name, sd.value, sd.chName, sd.typeName, sd.colName, sd.rkName ));
//		"%d,%f,%f,%d,%d,%d,%d,\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\""
		String[] qp = str.replace(Character.MIN_VALUE ,'\n').split( "\"" );
		this.name 		= empty2null( qp[1] );
		this.value 		= empty2null( qp[3] );
		this.chName 	= empty2null( qp[5] );
		this.typeName 	= empty2null( qp[7] );
		this.colName 	= empty2null( qp[9] );
		this.rkName 	= empty2null( qp[11] );
		this.color	 	= empty2null( qp[13] );
		qp = qp[0].split( "," );
		this.sdid 		= str2int( qp[0] );
		this.st 		= str2dbl( qp[1] );
		this.et 		= str2dbl( qp[2] );
		this.cid 		= str2int( qp[3] );
		this.tid 		= str2int( qp[4] );
		this.colid 		= str2int( qp[5] );
		this.rid 		= str2int( qp[6] );
		this.dl			= str2int( qp[7] );
	}

	/**
	 * Constructor
	 * @param st start time
	 * @param et end time
	 * @param cid channel id
	 * @param colid column id
	 * @param rid rank id
	 * @param tid type id
	 */
	public SuppDatum( Double st, Double et, int cid, int colid, int rid, int tid ) {
		sdid = 0;
		this.cid = cid;
		chName = null;
		this.colid = colid;
		colName = null;
		this.rid = rid;
		rkName = null;
		name = null;
		value = null;
		this.st = st;
		this.et = et;
		this.tid = tid;
		typeName = null;
		dl = -1;
	}

	/**
	 * Constructor
	 * @param st start time
	 * @param et end time
	 * @param chName channel name
	 * @param colName column name
	 * @param rkName rank name
	 * @param typeName type name
	 */
	public SuppDatum( Double st, Double et, String chName, String colName, String rkName, String typeName ) {
		sdid = 0;
		cid = -1;
		this.chName = chName;
		colid = -1;
		this.colName = colName;
		rid = -1;
		this.rkName = rkName;
		name = null;
		value = null;
		this.st = st;
		this.et = et;
		tid = -1;
		this.typeName = typeName;
		dl = -1;
	}

	/**
	 * Constructor
	 * @param rs resultset containing 1 row to init
	 * @throws SQLException
	 */
	public SuppDatum( ResultSet rs ) throws SQLException {
		sdid    = rs.getInt(1);
		tid     = rs.getInt(2);
		st      = rs.getDouble(3);
		et      = rs.getDouble(4);
		name    = rs.getString(5);
		value   = rs.getString(6);
		chName  = rs.getString(7);
		colName = rs.getString(8);
		rkName  = rs.getString(9);
		typeName= rs.getString(10);
		color   = rs.getString(11);
		
		cid     = rs.getInt(12);
		colid   = rs.getInt(13);
		rid     = rs.getInt(14);
		dl      = rs.getInt(15);
		
	}

	/**
	 * @return SuppDatum's xml representation
	 */
	public String toXML()
	{
		StringBuffer sb = new StringBuffer();
		sb.append("<suppdatum>\n");
		sb.append("<![CDATA[" + sdid + "\"" + tid + "\"" + st + "\"" + et + "\"" + 
			cid + "\"" + colid + "\"" + rid + "\"" + dl );
		String[] names = { name, value, chName, colName, rkName, typeName, color };
		for ( String s : names )
			sb.append( "\"" + (s==null ? "" : s) );
		sb.append("]]>\n</suppdatum>\n");
		return sb.toString();
	}
}	
	