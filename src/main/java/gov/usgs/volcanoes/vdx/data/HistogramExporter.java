package gov.usgs.volcanoes.vdx.data;

import gov.usgs.plot.render.HistogramRenderer;
import gov.usgs.volcanoes.vdx.data.Exportable;
import hep.aida.ref.Histogram1D;

/**
 * Add methods to HistogramRenderer to yield plot data for exporting.
 *
 * @author Scott Hunter, Bill Tollett
 */
public class HistogramExporter extends HistogramRenderer implements Exportable {

  protected Histogram1D myHistogram; // histogram to be exported
  protected int expIndex; // index of next bin to yield; >= bins if done
  protected int bins; // # of bins
  protected double time; // start time of next bin

  /**
   * Constructor for HistogramExporter.
   * 
   * @see HistogramRenderer
   */
  public HistogramExporter(Histogram1D h) {
    super(h);
    myHistogram = h;
    bins = h.xAxis().bins();
    resetExport();
  }

  /**
   * Reset export to beginning of histogram.
   */
  public void resetExport() {
    expIndex = 0;
    time = 0;
  }

  /**
   * Get next histogram row.
   * 
   * @return next row
   */
  public Double[] getNextExportRow() {
    if (expIndex >= bins && ((time + getMinX()) > getMaxX())) {
      return null;
    }

    Double[] row = new Double[2];
    row[0] = new Double(time) + getMinX();

    // Pad hypocenter list with 0s at beginning and end to line up with requested dates.
    if ((time + getMinX()) >= myHistogram.xAxis().lowerEdge()
        && (time + getMinX()) < myHistogram.xAxis().upperEdge()) {
      // Current date falls within histogram dates.
      row[1] = myHistogram.binHeight(expIndex);
      expIndex++;
    } else {
      System.out.println("PADDING!!!");
      row[1] = 0.0;
    }

    time += myHistogram.xAxis().binWidth(expIndex);
    return row;
  }

  /**
   * Return -1 (number of rows unknown).
   * 
   * @return -1
   */
  public int length() {
    return -1;
  }

}
