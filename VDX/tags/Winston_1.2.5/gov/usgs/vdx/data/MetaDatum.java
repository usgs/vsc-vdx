package gov.usgs.vdx.data;
	
/**
 * MetaDatum: one item of metadata (matching a line in a channelmetadata table), 
 * with ids and their translated names
 *
 * @author Scott Hunter
 */
public class MetaDatum
{
	public int cmid;		// Channel Metadata ID
	public int cid;			// Channel ID
	public String chName;	// CHannel NAME
	public int colid;		// COLumn ID
	public String colName;	// COLumn NAME
	public int rid;			// Rank ID
	public String rkName;	// RanK NAME
	public String name;		// Name of this piece of metadata
	public String value;	// Value of this piece of metadata
	
	/**
	 * MetaDatum default constructor
	 */
	public MetaDatum() {
	}
	
	/**
	 * MetaDatum constructor, just using IDs
	 *
	 * @param cid   Channel ID
	 * @param colid Column ID
	 * @param rid   Rank ID
	 */
	public MetaDatum( int cid, int colid, int rid ) {
		cmid = 0;
		this.cid = cid;
		chName = null;
		this.colid = colid;
		colName = null;
		this.rid = rid;
		rkName = null;
		name = null;
		value = null;
	}

	/**
	 * MetaDatum constructor, just using Names
	 *
	 * @param chName   Channel Name
	 * @param colName Column Name
	 * @param rkName   Rank Name
	 */
	public MetaDatum( String chName, String colName, String rkName ) {
		cmid = 0;
		cid = -1;
		this.chName = chName;
		colid = -1;
		this.colName = colName;
		rid = -1;
		this.rkName = rkName;
		name = null;
		value = null;
	}

}	
	