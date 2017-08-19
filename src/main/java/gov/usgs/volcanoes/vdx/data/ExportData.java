package gov.usgs.volcanoes.vdx.data;

/**
 * A sort of iterator for an Exportable source, yielding one row of Doubles at a time.
 * 
 * @author Scott Hunter, Bill Tollett
 */
public class ExportData implements Comparable<ExportData> {
  protected int nextRowGenIndex = -1; // index of next row of data
  protected int rowGenId = -1; // ID of this ED
  protected Double[] currRawRowData = null; // last row returned
  protected Double[] dummy = null; // a row of the proper number of nulls
  protected Exportable src; // source for data

  /**
   * Constructor.
   * 
   * @param id Integer indentifier
   * @param src Exportable source of data, returned one row of Doubles at a time
   */
  public ExportData(int id, Exportable src) {
    nextRowGenIndex = 0;
    rowGenId = id;
    this.src = src;
    nextExportDatum();
    dummy = new Double[currRawRowData.length];
  }

  /**
   * Yield the next row of data.
   * 
   * @return Double[] next row of data
   */
  public Double[] nextExportDatum() {
    currRawRowData = src.getNextExportRow();
    return currRawRowData;
  }

  /**
   * Yield the last row of data returned by nextExportDatum.
   * 
   * @return Double[] last row of data returned by nextExportDatum
   */
  public Double[] currExportDatum() {
    return currRawRowData;
  }

  /**
   * Yield ID.
   * 
   * @return int ID
   */
  public int exportDataId() {
    return rowGenId;
  }

  /**
   * Yield dummy datum (Double[] of proper length but all nulls).
   * 
   * @return Double[] array of nulls of proper length
   */
  public Double[] dummyExportDatum() {
    return dummy;
  }

  /**
   * Compare this is obj.
   * 
   * @param obj ExportObject to be compared to
   * @return int result of comparison
   * @see Comparable
   */
  public int compareTo(ExportData obj) {
    if (currRawRowData == null) {
      if (obj == null) {
        return 0;
      } else {
        return +1;
      }
    } else if (obj == null) {
      return -1;
    } else {
      Double[] eoCed = obj.currExportDatum();
      if (currRawRowData[0] == null) {
        if (eoCed[0] == null) {
          return 0;
        } else {
          return 0;
        }
      } else if (eoCed == null) {
        return -1;
      }
      int cmp = currRawRowData[0].compareTo(eoCed[0]);
      if (cmp != 0) {
        return cmp;
      } else {
        return rowGenId - obj.exportDataId();
      }
    }
  }

  /**
   * Yield # of rows, -1 if not known.
   * 
   * @return int # of rows, -1 if not known
   */
  public int count() {
    return src.length();
  }
}
