package gov.usgs.volcanoes.vdx.data.tensorstrain;

import cern.colt.matrix.DoubleFactory2D;
import cern.colt.matrix.DoubleMatrix2D;

import gov.usgs.volcanoes.core.data.GenericDataMatrix;

import java.util.List;

/**
 * GenericDataMatrix with 3 columns: time/east/north to store tilt data.
 *
 * @author Max Kokoulin
 */
public class TensorstrainData extends GenericDataMatrix {

  /**
   * Default constructor.
   */
  public TensorstrainData() {
    super();
  }

  /**
   * Constructor.
   *
   * @param pts list of raws, each of them is double[3]
   */
  public TensorstrainData(List<double[]> pts) {
    super(pts);
  }

  /**
   * Set predefined column names: time/east/north.
   */
  public void setColumnNames() {
    columnMap.put("time", 0);
    columnMap.put("rank", 1);
    columnMap.put("ch0", 2);
    columnMap.put("ch1", 3);
    columnMap.put("ch2", 4);
    columnMap.put("ch3", 5);
    columnMap.put("eEEpeNN", 6);
    columnMap.put("eEEmeNN", 7);
    columnMap.put("e2EN", 8);
    columnMap.put("barometer", 9);
    columnMap.put("rainfall", 10);
    columnMap.put("pore", 11);
  }

  /**
   * Convert the rainfall values to plottable time series values.
   */

  public DoubleMatrix2D getRainDataWithoutTime() {

    DoubleMatrix2D rain = DoubleFactory2D.dense.make(data.rows(), 1, Double.NaN);

    double total = 0;
    double r = 0;
    double last = data.getQuick(0, 10);

    // set the initial amount of rainfall to be zero for this time period
    rain.setQuick(0, 0, 0);

    // iterate through all subsequent rows and assign a rainfall amount if
    // the
    // data increases. Keep the total of the rainfall is less than the
    // previous reading
    for (int i = 1; i < data.rows(); i++) {
      r = data.getQuick(i, 10);
      if (!Double.isNaN(r)) {
        if (r < last) {
          last = 0;
        }
        total += (r - last);
        last = r;
        rain.setQuick(i, 0, total);
      }
    }
    ;
    return rain;
  }

  /**
   * Gets radial/tangential data using the specified azimuth.
   *
   * @param theta the azimuth in degrees
   * @return the Nx2 data matrix
   */
  public DoubleMatrix2D getAllData(double theta) {
    return DoubleFactory2D.dense.compose(new DoubleMatrix2D[][]{{
        data.viewPart(0, 0, data.rows(), 9),
        getRotatedDataWithoutTime(theta),
        data.viewPart(0, 9, data.rows(), 1),
        getRainDataWithoutTime(),
        data.viewPart(0, 11, data.rows(), 1)}});
  }

  /**
   * Gets radial/tangential data using the specified azimuth.
   *
   * @param theta the azimuth in degrees
   * @return the Nx2 data matrix
   */
  public DoubleMatrix2D getRotatedDataWithoutTime(double theta) {
    //DoubleMatrix2D en = data.viewPart(0, 7, data.rows(), 2);
    DoubleMatrix2D en = DoubleFactory2D.dense.make(data.rows(), 2);
    double tr = Math.toRadians(theta);
    for (int i = 0; i < data.rows(); i++) {
      en.setQuick(i, 0,
          data.getQuick(i, 7) * Math.cos(2 * tr) + data.getQuick(i, 8) * Math.sin(2 * tr));
      en.setQuick(i, 1,
          -data.getQuick(i, 7) * Math.sin(2 * tr) + data.getQuick(i, 8) * Math.cos(2 * tr));
    }
    return en;
  }
}
