package gov.usgs.vdx.data.wave;

import gov.usgs.math.DownsamplingType;
import gov.usgs.plot.data.BinaryDataSet;
import gov.usgs.util.UtilException;
import gov.usgs.vdx.data.VDXSource;

/**
 * 
 * @author Dan Cervelli
 */
public class WaveSource extends VDXSource {
	
	public String getType() {
		return "wave";
	}
	
	public void disconnect() {
		defaultDisconnect();
	}

	protected BinaryDataSet getData(String channel, double st, double et, int maxrows, DownsamplingType ds, int dsInt) throws UtilException {
		return data.getWave(channel, st, et, maxrows);
	}
}
