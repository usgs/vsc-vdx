package gov.usgs.volcanoes.vdx.data.rsam;

import gov.usgs.volcanoes.core.data.RSAMData;
import gov.usgs.volcanoes.core.data.Scnl;
import gov.usgs.volcanoes.core.math.DownsamplingType;
import gov.usgs.volcanoes.core.util.UtilException;
import gov.usgs.volcanoes.vdx.data.VDXSource;
import gov.usgs.volcanoes.vdx.server.BinaryResult;
import gov.usgs.volcanoes.vdx.server.RequestResult;
import gov.usgs.volcanoes.vdx.server.TextResult;
import gov.usgs.volcanoes.winston.Channel;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * RSAM Datasource.
 *
 * @author Dan Cervelli
 * @author Bill Tollett
 */
public class RsamSource extends VDXSource {

  public String getType() {
    return "rsam";
  }

  public void disconnect() {
    defaultDisconnect();
  }

  /**
   * Get data.
   *
   * @param params request parameters
   * @return RequestResult
   */
  public RequestResult getData(Map<String, String> params) {

    String action = params.get("action");

    if (action.equals("channels")) {
      List<Channel> chs   = channels.getChannels();
      List<String> result = new ArrayList<String>();
      for (Channel ch : chs) {
        result.add(ch.toVDXString());
      }
      return new TextResult(result);

    } else if (action.equals("ratdata")) {
      String cids         = params.get("ch");
      double st           = Double.parseDouble(params.get("st"));
      double et           = Double.parseDouble(params.get("et"));
      DownsamplingType ds = DownsamplingType.fromString(params.get("ds"));
      int dsInt           = Integer.parseInt(params.get("dsInt"));
      RSAMData data       = null;
      try {
        data = getRatSamData(cids, st, et, getMaxRows(), ds, dsInt);
      } catch (UtilException e) {
        return getErrorResult(e.getMessage());
      }
      if (data != null) {
        return new BinaryResult(data);
      }

    } else if (action.equals("data")) {
      int cid             = Integer.parseInt(params.get("ch"));
      double st           = Double.parseDouble(params.get("st"));
      double et           = Double.parseDouble(params.get("et"));
      DownsamplingType ds = DownsamplingType.fromString(params.get("ds"));
      int dsInt           = Integer.parseInt(params.get("dsInt"));

      String code   = channels.getChannelCode(cid);
      RSAMData data = null;
      try {
        data = getData(code, st, et, getMaxRows(), ds, dsInt);
      } catch (UtilException e) {
        return getErrorResult(e.getMessage());
      }
      if (data != null) {
        return new BinaryResult(data);
      }
    }

    return null;
  }

  protected RSAMData getData(String code, double st, double et, int maxrows, DownsamplingType ds,
      int dsInt) throws UtilException {
    Scnl scnl = Scnl.parse(code);
    return data.getRSAMData(scnl, st, et, maxrows, ds, dsInt);
  }

  protected RSAMData getRatSamData(String code, double st, double et, int maxrows,
      DownsamplingType ds, int dsInt) throws UtilException {
    RSAMData result1 = null;
    RSAMData result2 = null;

    String[] codes = code.split(",");
    String code1   = channels.getChannelCode(Integer.parseInt(codes[0]));
    String code2   = channels.getChannelCode(Integer.parseInt(codes[1]));
    result1        = getData(code1, st, et, maxrows, ds, dsInt);
    result2        = getData(code2, st, et, maxrows, ds, dsInt);

    return result1.getRatSAM(result2);
  }
}
