package gov.usgs.volcanoes.vdx.data;

import cern.colt.matrix.DoubleMatrix2D;

import gov.usgs.volcanoes.core.legacy.plot.render.MatrixRenderer;
import gov.usgs.volcanoes.vdx.data.Exportable;

import java.util.Map;


/**
 * Add methods to MatrixRenderer to yield plot data for exporting.
 *
 * @author Scott Hunter
 */
public class MatrixExporter extends MatrixRenderer implements Exportable {

  protected int numCols; // size of each exported row
  protected boolean[] visible; // col i of data is visible
  protected int expIndex; // index of next row to yield
  protected double timeOffset; // Offset to add to row time of each row yielded

  /**
   * Constructor.
   * 
   * @param axisMap Mapping of column indices to header names
   * @param offset Offset to add to row times
   * @see MatrixRenderer
   */
  public MatrixExporter(DoubleMatrix2D d, boolean ranks, Map<Integer, String> axisMap,
      double offset) {
    super(d, ranks);
    initMe(d, ranks, axisMap, offset);
  }

  /**
   * Constructor.
   * 
   * @param axisMap Mapping of column indices to header names
   * @see MatrixRenderer
   */
  public MatrixExporter(DoubleMatrix2D d, boolean ranks, Map<Integer, String> axisMap) {
    super(d, ranks);
    initMe(d, ranks, axisMap, 0);
  }

  /**
   * Constructor.
   * 
   * @see MatrixRenderer
   */
  public MatrixExporter(DoubleMatrix2D d, boolean ranks) {
    super(d, ranks);
    initMe(d, ranks, null, 0);
  }

  /**
   * Common initialization code.
   * 
   * @param d DoubleMatrix2D the actual data
   * @param ranks = "multiple ranks employed"
   * @param axisMap Mapping of column indices to header names
   * @param offset Offset to add to row times
   */
  private void initMe(DoubleMatrix2D d, boolean ranks, Map<Integer, String> axisMap,
      double offset) {
    timeOffset = offset;
    if (axisMap == null) {
      visible = new boolean[1];
      visible[0] = true;
      numCols = 1;
    } else {
      visible = new boolean[axisMap.size()];
      numCols = 0;
      for (int i = 0; i < visible.length; i++) {
        if (axisMap.get(i).equals("")) {
          visible[i] = false;
        } else {
          visible[i] = true;
          numCols++;
        }
      }
    }
    resetExport();
  }

  /**
   * Reset export to beginning of matrix.
   */
  public void resetExport() {
    expIndex = 0;
  }

  /**
   * Get next data column to export.
   * 
   * @return next column
   */
  public Double[] getNextExportRow() {
    if (expIndex >= getData().rows()) {
      return null;
    }
    Double[] row = new Double[numCols + 1];
    row[0] = getData().getQuick(expIndex, 0) + timeOffset;
    int jr = 0;
    for (int j = 0; j < visible.length; j++) {
      if (visible[j]) {
        row[jr + 1] = getData().getQuick(expIndex, j + getOffset());
        jr++;
      }
    }
    expIndex++;
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
