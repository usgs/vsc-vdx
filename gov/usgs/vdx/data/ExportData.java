package gov.usgs.vdx.data;

/**
 * A sort of iterator for an Exportable source, 
 * yielding one row of Doubles at a time
 * @author Scott Hunter
 */
public class ExportData implements Comparable
{
	protected int nextRowGenIndex = -1;			// index of next row of data
	protected int rowGenID = -1;				// ID of this ED
	protected Double[] currRawRowData = null;	// last row returned
	protected Double[] dummy = null;			// a row of the proper number of nulls
	protected Exportable src;					// source for data

	/**
	 * Constructor
	 * @param id Integer indentifier
	 * @param src Exportable source of data, returned one row of Doubles at a time
	 */
	public ExportData( int id, Exportable src )
	{
		nextRowGenIndex = 0;
		rowGenID = id;
		this.src = src;
		nextExportDatum();
		dummy = new Double[currRawRowData.length];
	}
	
	/**
	 * Yield the next row of data
	 * @returns Double[] next row of data
	 */
	public Double[] nextExportDatum()
	{
		currRawRowData = src.getNextExportRow();
		return currRawRowData;
	}
	
	/**
	 * Yield the last row of data returned by nextExportDatum
	 * @returns Double[] last row of data returned by nextExportDatum
	 */
	public Double[] currExportDatum()
	{
		return currRawRowData;
	}
	
	/**
	 * Yield ID
	 * @returns int ID
	 */
	public int exportDataID() 
	{
		return rowGenID;
	}
	
	/**
	 * Yield dummy datum (Double[] of proper length but all nulls)
	 * @returns Double[] array of nulls of proper length
	 */
	public Double[] dummyExportDatum() {
		return dummy;
	}
	
	/**
	 * Compare this is obj
	 * @param obj ExportObject to be compared to
	 * @returns int result of comparison
	 * @see Comparable
	 */
	public int compareTo( Object obj )
	{
		ExportData eo = (ExportData)obj;
		if ( currRawRowData == null )
			if ( eo == null )
				return 0;
			else
				return +1;
		else if ( eo == null )
			return -1;
		else {
			Double[] eo_ced = eo.currExportDatum();
			if ( currRawRowData[0] == null )
				if ( eo_ced[0] == null )
					return 0;
				else
					return 0;
			else if ( eo_ced == null )
				return -1;
			int cmp = currRawRowData[0].compareTo( eo_ced[0] );
			if ( cmp != 0 )
				return cmp;
			else
				return rowGenID - eo.exportDataID();
		}
	}}
