package gov.usgs.vdx.data;

/**
 * Interface for iterating through data to be exported 
 * @author Scott Hunter
 */
public interface Exportable
{
	/**
	 * Restart at beginning of data
	 */
	public void resetExport();
	
	/**
	 * Yield next row of data (array of Doubles)
	 * @return Double[] next row of data
	 */
	public Double[] getNextExportRow();

}
