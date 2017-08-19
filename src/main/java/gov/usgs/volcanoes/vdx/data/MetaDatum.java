package gov.usgs.volcanoes.vdx.data;

/**
 * MetaDatum: one item of metadata (matching a line in a channelmetadata table), with ids and their
 * translated names.
 *
 * @author Scott Hunter, Bill Tollett
 */
public class MetaDatum {
  public int cmid; // Channel Metadata ID
  public int cid; // Channel ID
  public String chName; // CHannel NAME
  public int colid; // COLumn ID
  public String colName; // COLumn NAME
  public int rid; // Rank ID
  public String rkName; // RanK NAME
  public String name; // Name of this piece of metadata
  public String value; // Value of this piece of metadata

  /**
   * MetaDatum default constructor.
   */
  public MetaDatum() {}

  /**
   * MetaDatum constructor, just using IDs.
   *
   * @param cid Channel ID
   * @param colid Column ID
   * @param rid Rank ID
   */
  public MetaDatum(int cid, int colid, int rid) {
    cmid = 0;
    this.cid = cid;
    chName = null;
    this.colid = colid;
    colName = null;
    this.rid = rid;
    rkName = null;
    name = null;
    value = null;
  }

  /**
   * MetaDatum constructor, just using Names.
   *
   * @param chName Channel Name
   * @param colName Column Name
   * @param rkName Rank Name
   */
  public MetaDatum(String chName, String colName, String rkName) {
    cmid = 0;
    cid = -1;
    this.chName = chName;
    colid = -1;
    this.colName = colName;
    rid = -1;
    this.rkName = rkName;
    name = null;
    value = null;
  }

  /**
   * Constructor String rep of MetaDatum is a CSV with: - strings each enclosed in quotes - all
   * MIN_VALUE characters will be translated to newlines - component order: cmid, cid, colid, rid,
   * name, value, chName, colName, rkName.
   * 
   * @param str CSV rep of a MetaDatum
   */
  public MetaDatum(String str) {
    String[] qp = str.replace(Character.MIN_VALUE, '\n').split("\"");
    this.name = qp[1];
    this.value = qp[3];
    this.chName = qp[5];
    this.colName = qp[7];
    this.rkName = qp[9];
    qp = qp[0].split(",");
    this.cmid = Integer.parseInt(qp[0]);
    this.cid = Integer.parseInt(qp[1]);
    this.colid = Integer.parseInt(qp[2]);
    this.rid = Integer.parseInt(qp[3]);
  }

  /**
   * Return xml representation of MetaDatum.
   * 
   * @return MetaDatum's xml representation
   */
  public String toXML() {
    StringBuffer sb = new StringBuffer();
    sb.append("<metadatum>\n");
    sb.append("<![CDATA[" + cmid + "\"" + cid + "\"" + colid + "\"" + rid);
    String[] names = {name, value, chName, colName, rkName};
    for (String s : names) {
      sb.append("\"" + (s == null ? "" : s));
    }
    sb.append("]]>\n</metadatum>\n");
    return sb.toString();
  }
}

