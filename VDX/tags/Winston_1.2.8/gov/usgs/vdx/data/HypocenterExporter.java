package gov.usgs.vdx.data;

import gov.usgs.vdx.data.Exportable;
import gov.usgs.vdx.data.hypo.Hypocenter;
import gov.usgs.vdx.data.hypo.HypocenterList;

import java.util.List;


/**
 * Class to yield hypo data for exporting
 *
 * @author Scott Hunter
 */
public class HypocenterExporter implements Exportable {

	protected List<Hypocenter> hypos;		// list of hypos to export
    protected int expIndex;         	// index of next hypo to yield; >= count if done
    protected int count;				// # of hypos
    protected boolean exportAll;		// ="export all columns"
    
    /**
     * Constructor
     * @see gov.usgs.plot.render.Plot.HistogramRenderer
     */
    public HypocenterExporter(HypocenterList hypos, boolean exportAll) {
    	this.hypos = hypos.getHypocenters();
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
		if ( exportAll )
			row = new Double[16];
		else
			row = new Double[5];
		Hypocenter h = hypos.get(expIndex);
		row[0] = h.j2ksec;
		row[1] = h.lat;
		row[2] = h.lon;
		row[3] = h.depth;
		row[4] = h.prefmag;
		if ( exportAll ) {
			row[5] = fixForExport(h.ampmag);
			row[6] = fixForExport(h.codamag);
			row[7] = fixForExport(h.nphases);
			row[8] = fixForExport(h.azgap);
			row[9] = fixForExport(h.dmin);
			row[10] = fixForExport(h.rms);
			row[11] = fixForExport(h.nstimes);
			row[12] = fixForExport(h.herr);
			row[13] = fixForExport(h.verr);
			row[14] = fixForExport(h.magtype);
			row[15] = fixForExport(h.rmk);
		}
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
