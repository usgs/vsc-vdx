package gov.usgs.vdx.data.lightning;

import gov.usgs.vdx.data.Exportable;

import java.util.List;


/**
 * Class to yield hypo data for exporting
 *
 * @author Scott Hunter
 */
public class LightningExporter implements Exportable {

	protected List<Stroke> hypos;		// list of hypos to export
    protected int expIndex;         	// index of next hypo to yield; >= count if done
    protected int count;				// # of hypos
    protected boolean exportAll;		// ="export all columns"
    
    /**
     * Constructor
     * @see gov.usgs.plot.render.Plot.HistogramRenderer
     */
    public LightningExporter(StrokeList hypos, boolean exportAll) {
    	this.hypos = hypos.getStrokes();
    	count = hypos.size();
    	resetExport();
    	this.exportAll = exportAll;
    }
    
    /**
     * Reset export to beginning of hypocenter
     */
	public void resetExport() {
		expIndex = 0;
	}
	
    /**
     * return a Double representation of Double val suitable for export
     */
	private Double fixForExport( Double val ) {
		if ( val == null )
			return Double.NaN;
		else
			return val;
	}

    /**
     * return a Double representation of Integer val suitable for export
     */
	private Double fixForExport( Integer val ) {
		if ( val == null )
			return Double.NaN;
		else
			return (Double)((double)((int)val));
	}

    /**
     * return a Double representation of String val suitable for export
     */
	private Double fixForExport( String val ) {
		if ( val == null || val.length()==0 )
			return Double.NaN;
		else
			return (Double)((double)(int)val.charAt(0));
	}
	/**
	 * Get data about next hypocenter for export
	 * @return next hypocenter
	 */
	public Double[] getNextExportRow() {
		if ( expIndex >= count ) {
			return null;
		}
		Double[] row;
		row = new Double[5];
		Stroke h = hypos.get(expIndex);
		row[0] = h.j2ksec;
		row[1] = h.lat;
		row[2] = h.lon;
		row[3] = (double) h.stationsDetected;
		row[4] = h.residual;
		expIndex++;
		return row;
	}

	/**
	 * Return # of hypos
	 * @return # of hypos
	 */
	public int length() {
		return count;
	}
}
