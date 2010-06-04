package gov.usgs.vdx.data;

import gov.usgs.vdx.data.wave.Wave;

import gov.usgs.vdx.data.wave.SliceWave;
import gov.usgs.vdx.data.wave.plot.SliceWaveRenderer;
import gov.usgs.vdx.data.Exportable;


/**
 * Add methods to SliceWaveRenderer to yield plot data for exporting
 *
 * @author Scott Hunter
 */
public class SliceWaveExporter extends SliceWaveRenderer implements Exportable {

    private double step = 0, time = 0, bias = 0;

	public Double[] getNextExportRow() {
		if ( wave == null )
			return null;
		double value = 0;
		if ( step == 0 ) {
			resetExport();
			step = (1 / wave.getSamplingRate());
			time = wave.getStartTime();
			if ( removeBias )
				bias = wave.mean();
		}
		while ( wave.hasNext() ) {
			value = wave.next();
			time += step;
			if ( value != Wave.NO_DATA ) {
				Double[] retval = new Double[2];
				retval[0] = time - step;
				retval[1] = value - bias;
				return retval;
			}
		}
		return null;
	}

	public void resetExport() {
		if ( wave != null )
			wave.reset();
	}
	
}
