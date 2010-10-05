package gov.usgs.vdx.data;

import gov.usgs.plot.MatrixRenderer;
import gov.usgs.vdx.data.Exportable;

import cern.colt.matrix.DoubleMatrix2D;
import java.util.Map;


/**
 * Add methods to MatrixRenderer to yield plot data for exporting
 *
 * @author Scott Hunter
 */
public class MatrixExporter extends MatrixRenderer implements Exportable {

    protected int numCols;			// size of each exported row
    protected boolean visible[];	// col i of data is visible
    protected int expIndex;  		// index of next row to yield
    protected double timeOffset;	// Offset to add to row time of each row yielded
    
    /**
     * Constructor
     * @param axisMap Mapping of column indices to header names
     * @param time_offset Offset to add to row times
     * @see MatrixRenderer
     */   
	public MatrixExporter(DoubleMatrix2D d, boolean ranks, Map<Integer,String> axisMap, double time_offset) {
		super( d, ranks );
		init_me( d, ranks, axisMap, time_offset );
	}
	
    /**
     * Constructor
     * @param axisMap Mapping of column indices to header names
     * @see MatrixRenderer
     */   
	public MatrixExporter(DoubleMatrix2D d, boolean ranks, Map<Integer,String> axisMap) {
		super( d, ranks );
		init_me( d, ranks, axisMap, 0 );
	}
	
    /**
     * Constructor
     * @see MatrixRenderer
     */   
	public MatrixExporter(DoubleMatrix2D d, boolean ranks) {
		super( d, ranks );
		init_me( d, ranks, null, 0 );
	}
	
	/**
	 * Common initialization code
	 * @param d DoubleMatrix2D the actual data
	 * @param ranks = "multiple ranks employed"
     * @param axisMap Mapping of column indices to header names
     * @param time_offset Offset to add to row times
	 */
	private void init_me( DoubleMatrix2D d, boolean ranks, Map<Integer,String> axisMap, double time_offset ) {
		timeOffset = time_offset;
		if ( axisMap == null ) {
			visible = new boolean[ 1 ];
			visible[0] = true;
			numCols = 1;
		} else {
			visible = new boolean[ axisMap.size() ];
			numCols = 0;
			for ( int i = 0; i< visible.length; i++ )
				if ( axisMap.get(i).equals("") )
					visible[i] = false;
				else {
					visible[i] = true;
					numCols++;
				}
		}
		resetExport();
	}

    /**
     * Reset export to beginning of matrix
     */
	public void resetExport() {
		expIndex = 0;
	}
	
	/**
	 * Get next data column to export
	 */
	public Double[] getNextExportRow() {
		if ( expIndex >= getData().rows() )
			return null;
		Double[] row = new Double[numCols+1];
		row[0] = getData().getQuick(expIndex, 0) + timeOffset;
		int jr = 0;
		for (int j = 0; j < visible.length; j++)
			if ( visible[j] ) {
				row[jr+1] = getData().getQuick(expIndex, jr + getOffset());
				jr++;
			}
		expIndex++;
		return row;
	}

}
