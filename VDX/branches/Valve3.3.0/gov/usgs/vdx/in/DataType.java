package gov.usgs.vdx.in;

/**
 * DataType data structure.  Used for labeling collections of data from a data stream for input into a database
 *
 * @author Loren Antolik
 */
public class DataType {

	public String	dataType;
	public int		order;
	public String	type;
	public String	format;
	public int		rank;
	
	/**
	 * Default constructor
	 */
	public DataType(){}
	
	/**
	 * Constructor
	 * @param dataType	label for this collection of data within the data line
	 * @param order		order for this collection of data within the data line
	 * @param type		type of data in this collection. data, timestamp, ignore
	 * @param format	format of data in this collection
	 * @param rank		database rank of data in this collection
	 */
	public DataType (String dataType, int order, String type, String format, int rank) {
		this.dataType	= dataType;
		this.order		= order;
		this.type		= type;
		this.format		= format;
		this.rank		= rank;
	}
}