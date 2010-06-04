package gov.usgs.vdx.data;

import hep.aida.ref.Histogram1D;

import gov.usgs.plot.HistogramRenderer;
import gov.usgs.vdx.data.Exportable;

import cern.colt.matrix.DoubleMatrix2D;

import java.util.Map;


/**
 * Add methods to HistogramRenderer to yield plot data for exporting
 *
 * @author Scott Hunter
 */
public class HistogramExporter extends HistogramRenderer implements Exportable {

    
    protected Histogram1D myHistogram;	// histogram to be exported
    protected int expIndex;         	// index of next bin to yield; >= bins if done
    protected int bins;					// # of bins
    protected double time;				// start time of next bin
    
    /**
     * Constructor
     * @see HistogramRenderer
     */
    public HistogramExporter(Histogram1D h) {
    	super( h );
    	myHistogram = h;
    	bins = h.xAxis().bins();
    	resetExport();
    }
    
	public void resetExport() {
		expIndex = 0;
		time = 0;
	}
	
	public Double[] getNextExportRow() {
		if ( expIndex >= bins ) {
			return null;
		}
		Double[] row = new Double[2];
		row[0] = new Double(time) + getMinX();
		time += myHistogram.xAxis().binWidth(expIndex);
		row[1] = myHistogram.binHeight(expIndex);
		expIndex++;
		return row;
	}
}
