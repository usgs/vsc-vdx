package gov.usgs.volcanoes.vdx.data.wave;

import gov.usgs.volcanoes.core.data.BinaryDataSet;
import gov.usgs.volcanoes.core.data.Scnl;
import gov.usgs.volcanoes.core.math.DownsamplingType;
import gov.usgs.volcanoes.core.util.UtilException;
import gov.usgs.volcanoes.vdx.data.VDXSource;

/**
 * Helicorder Datasource.
 *
 * @author Dan Cervelli
 * @author Bill Tollett
 */
public class HelicorderSource extends VDXSource {

  public String getType() {
    return "helicorder";
  }

  public void disconnect() {
    defaultDisconnect();
  }

  protected BinaryDataSet getData(String channel, double st, double et, int maxrows,
      DownsamplingType ds, int dsInt) throws UtilException {
    Scnl scnl = Scnl.parse(channel);
    return data.getHelicorderData(scnl, st, et, maxrows);
  }
}
