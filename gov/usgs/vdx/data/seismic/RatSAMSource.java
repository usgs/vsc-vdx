package gov.usgs.vdx.data.seismic;

import gov.usgs.math.DownsamplingType;
import gov.usgs.plot.data.BinaryDataSet;
import gov.usgs.plot.data.RSAMData;
import gov.usgs.util.UtilException;
import gov.usgs.vdx.data.VDXSource;

/**
 * 
 * $Log: not supported by cvs2svn $
 *
 * @author Tom Parker
 */
public class RatSAMSource extends VDXSource
{
	public String getType()
	{
		return "rsam";
	}
	
	public void disconnect() {
		defaultDisconnect();
	}

	protected BinaryDataSet getData(String channel, double st, double et, int maxrows, DownsamplingType ds, int dsInt) throws UtilException
	{
		RSAMData d1 = null;
		RSAMData d2 = null;
		String[] channels = channel.split(",");
		if (channels.length == 2)
		{
			d1 = data.getRSAMData(channels[0], st, et, maxrows, ds, dsInt);
			d2 = data.getRSAMData(channels[1], st, et, maxrows, ds, dsInt);
		}
		
		return (BinaryDataSet) d1.getRatSAM(d2);
	}
}
