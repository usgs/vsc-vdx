/*
importData:
	* need to parse comments for info about upload
		* source
		* start & end times (needed?)
		* sampling-rate & datatype for Winston
	* parse headers for columns
		* could determine which columns from URL, but not order
	* use ImportFile
		* build config file from info above
			* # headerlines
			* timezone
			* timestampmask?
			* importCols
			* channel(s)
			* datasource(s)
importMeta:
	* extract datasource from filename
		* rest from data
	* easy parse (if no " to worry about)
	* easy post to DB

exportMeta:
	* easy!
	
	select Chan.name, Meta.name, Meta.value [, Col.name] [, Rank.name]
	from channelmetadata as Meta, Channels as Chan [, columns as Col] [, ranks as Rank]
	where Chan.cid = Meta.cid [and Col.colid = Meta.colid] [and Rank.rid = Meta.rid];
*/	
package gov.usgs.vdx.data;
	
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

	/*
	public SuppDatum( int cmid, int cid, int colid, int rid, String name, String value ) {
		this.cmid = cmid;
		this.cid = cid;
		chName = null;
		this.colid = colid;
		colName = null;
		this.rid = rid;
		rkName = null;
		this.name = name;
		this.value = value;
	}
	*/
}	
	