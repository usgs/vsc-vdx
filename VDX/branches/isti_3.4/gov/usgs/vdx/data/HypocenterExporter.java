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
    
    /**
     * Constructor
     * @see gov.usgs.Plot.HistogramRenderer
     */
    public HypocenterExporter(HypocenterList hypos) {
    	this.hypos = hypos.getHypocenters();
    	count = hypos.size();
    	resetExport();
    }
    
    /**
     * Reset export to beginning of hypocenter
     */
	public void resetExport() {
		expIndex = 0;
	}
	
	/**
	 * Get data about next hypocenter for export
	 * @return next hypocenter
	 */
	public Double[] getNextExportRow() {
		if ( expIndex >= count ) {
			return null;
		}
		Double[] row = new Double[5];
		Hypocenter h = hypos.get(expIndex);
		row[0] = h.j2ksec;
		row[1] = h.lat;
		row[2] = h.lon;
		row[3] = h.depth;
		row[4] = h.prefmag;
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
