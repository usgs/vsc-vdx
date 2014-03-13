package gov.usgs.vdx.data.seismic;

import gov.usgs.math.DownsamplingType;
import gov.usgs.plot.data.BinaryDataSet;
import gov.usgs.util.UtilException;
import gov.usgs.vdx.data.VDXSource;

/**
 * 
 * @author Dan Cervelli
 */
public class HelicorderSource extends VDXSource {
	
	public String getType() {
		return "helicorder";
	}
	
	public void disconnect() {
		defaultDisconnect();
	}
	
	protected BinaryDataSet getData(String channel, double st, double et, int maxrows, DownsamplingType ds, int dsInt) throws UtilException{
		return data.getHelicorderData(channel, st, et, maxrows);
	}
}
