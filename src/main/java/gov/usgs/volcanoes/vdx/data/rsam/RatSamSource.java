package gov.usgs.volcanoes.vdx.data.rsam;

import gov.usgs.volcanoes.core.data.BinaryDataSet;
import gov.usgs.volcanoes.core.data.RSAMData;
import gov.usgs.volcanoes.core.data.Scnl;
import gov.usgs.volcanoes.core.math.DownsamplingType;
import gov.usgs.volcanoes.core.util.UtilException;
import gov.usgs.volcanoes.vdx.data.VDXSource;

/**
 * RatSAM datasource.
 *
 * @author Tom Parker
 * @author Bill Tollett
 */
public class RatSamSource extends VDXSource {

  public String getType() {
    return "rsam";
  }

  public void disconnect() {
    defaultDisconnect();
  }

  protected BinaryDataSet getData(String channel, double st, double et, int maxrows,
      DownsamplingType ds, int dsInt) throws UtilException {
    RSAMData d1       = null;
    RSAMData d2       = null;
    String[] channels = channel.split(",");
    if (channels.length == 2) {
      d1 = data.getRSAMData(Scnl.parse(channels[0]), st, et, maxrows, ds, dsInt);
      d2 = data.getRSAMData(Scnl.parse(channels[1]), st, et, maxrows, ds, dsInt);
    }

    return (BinaryDataSet) d1.getRatSAM(d2);
  }
}
