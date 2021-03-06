package gov.usgs.volcanoes.vdx.data;

import gov.usgs.volcanoes.core.configfile.ConfigFile;
import gov.usgs.volcanoes.core.data.BinaryDataSet;
import gov.usgs.volcanoes.core.math.DownsamplingType;
import gov.usgs.volcanoes.core.time.J2kSec;
import gov.usgs.volcanoes.core.util.StringUtils;
import gov.usgs.volcanoes.core.util.UtilException;
import gov.usgs.volcanoes.vdx.server.BinaryResult;
import gov.usgs.volcanoes.vdx.server.RequestResult;
import gov.usgs.volcanoes.vdx.server.TextResult;
import gov.usgs.volcanoes.winston.Channel;
import gov.usgs.volcanoes.winston.db.Channels;
import gov.usgs.volcanoes.winston.db.Data;
import gov.usgs.volcanoes.winston.db.WinstonDatabase;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

/**
 * A base-class for VDX to directly access Winston databases.
 *
 * @author Dan Cervelli, Bill Tollett
 */
public abstract class VDXSource implements DataSource {

  protected WinstonDatabase winston;
  protected Data data;
  protected Channels channels;
  protected String vdxName;
  private int maxrows = 0;

  /**
   * Initialize the datasource.
   * 
   * @param cf config file for use in initialization
   */
  public void initialize(ConfigFile cf) {
    if (winston == null) {
      String driver = cf.getString("driver");
      String prefix = cf.getString("prefix");
      String url = cf.getString("url");
      vdxName = cf.getName();
      maxrows = StringUtils.stringToInt(cf.getString("maxrows"), 0);
      int cacheCap = StringUtils.stringToInt(cf.getString("statementCacheCap"), 100);
      winston = new WinstonDatabase(driver, url, prefix, cacheCap);
    }
    data = new Data(winston);
    data.setVdxName(vdxName);
    channels = new Channels(winston);
  }

  /**
   * Close the datasource.
   */
  public void defaultDisconnect() {
    winston.close();
  }
  
  public abstract void disconnect();

  protected abstract BinaryDataSet getData(String channel, double st, double et, int maxrows,
      DownsamplingType ds, int dsInt) throws UtilException;

  /**
   * Get data.
   * 
   * @param params request parameters
   */
  public RequestResult getData(Map<String, String> params) {

    String action = params.get("action");

    // don't initially check that the action is not null, because not all requests define this
    if (action.equals("channels")) {
      List<Channel> chs = channels.getChannels();
      List<String> result = new ArrayList<String>();
      for (Channel ch : chs) {
        result.add(ch.toVDXString());
      }
      return new TextResult(result);

    } else if (action.equals("data")) {
      int cid = Integer.parseInt(params.get("ch"));
      double st = Double.parseDouble(params.get("st"));
      double et = Double.parseDouble(params.get("et"));
      DownsamplingType ds = DownsamplingType.fromString(
          StringUtils.stringToString(params.get("ds"), "None"));
      int dsInt = StringUtils.stringToInt(params.get("dsInt"), 0);
      String code = channels.getChannelCode(cid);
      BinaryDataSet data = null;
      try {
        data = getData(code, st, et, getMaxRows(), ds, dsInt);
      } catch (UtilException e) {
        return getErrorResult(e.getMessage());
      }
      if (data != null) {
        return new BinaryResult(data);
      }

    } else if (action.equals("suppdata")) {
      double st;
      double et;
      String arg = null;
      List<SuppDatum> data = null;
      SuppDatum suppDatum;

      String tz = params.get("tz");
      if (tz == null || tz.equals("")) {
        tz = "UTC";
      }
      SimpleDateFormat df = new SimpleDateFormat("yyyyMMddHHmmssSSS");
      df.setTimeZone(TimeZone.getTimeZone(tz));

      try {
        arg = params.get("st");
        st = J2kSec.fromDate(df.parse(arg));
        arg = params.get("et");
        if (arg == null || arg.equals("")) {
          et = Double.MAX_VALUE;
        } else {
          et = J2kSec.fromDate(df.parse(arg));
        }
      } catch (Exception e) {
        return getErrorResult("Illegal time string: " + arg + ", " + e);
      }

      arg = params.get("byID");
      if (arg != null && arg.equals("true")) {
        suppDatum = new SuppDatum(st, et, -1, -1, -1, -1);
        String[] args = {"ch", "col", "rk", "type"};
        for (int i = 0; i < 4; i++) {
          arg = params.get(args[i]);
          if (arg == null || arg.equals("")) {
            args[i] = null;
          } else {
            args[i] = arg;
          }
        }
        suppDatum.chName = args[0];
        if (suppDatum.chName != null) {
          suppDatum.cid = 0;
        }
        suppDatum.colName = args[1];
        if (suppDatum.colName != null) {
          suppDatum.colid = 0;
        }
        suppDatum.rkName = args[2];
        if (suppDatum.rkName != null) {
          suppDatum.rid = 0;
        }
        suppDatum.typeName = args[3];
        if (suppDatum.typeName != null) {
          suppDatum.tid = 0;
        }
      } else {
        String chName = params.get("ch");
        String colName = params.get("col");
        String rkName = params.get("rk");
        String typeName = params.get("type");
        suppDatum = new SuppDatum(st, et, chName, colName, rkName, typeName);
      }
      arg = params.get("dl");
      if (arg != null) {
        suppDatum.dl = Integer.parseInt(arg);
      }
      try {
        data = getMatchingSuppData(suppDatum, false);
      } catch (Exception e) {
        return getErrorResult(e.getMessage());
      }
      if (data != null) {
        List<String> result = new ArrayList<String>();
        for (SuppDatum sd : data) {
          result.add(String.format(
              "%d,%1.3f,%1.3f,%d,%d,%d,%d,%d,\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\"",
              sd.sdid, sd.st, sd.et, sd.cid, sd.tid, sd.colid, sd.rid, sd.dl, sd.name,
              sd.value.replace('\n', Character.MIN_VALUE), sd.chName, sd.typeName, sd.colName,
              sd.rkName, sd.color));
        }
        return new TextResult(result);
      }
      return null;
    } else if (action.equals("metadata")) {
      String arg = params.get("byID");
      List<MetaDatum> data = null;
      MetaDatum metaDatum;
      if (arg != null && arg.equals("true")) {
        int cid = Integer.parseInt(params.get("ch"));
        metaDatum = new MetaDatum(cid, -1, -1);
      } else {
        String chName = params.get("ch");
        metaDatum = new MetaDatum(chName, null, null);
      }

      try {
        data = getMatchingMetaData(metaDatum, false);
      } catch (Exception e) {
        return getErrorResult(e.getMessage());
      }
      if (data != null) {
        List<String> result = new ArrayList<String>();
        for (MetaDatum md : data) {
          result.add(md.cmid + "," + md.cid + ",\"" + md.name + "\",\"" + md.value + "\",\""
              + md.chName + "\"");
        }
        return new TextResult(result);
      }
    } else if (action.equals("supptypes")) {
      try {
        return getSuppTypes(true);
      } catch (Exception e) {
        return getErrorResult(e.getMessage());
      }
    }
    return null;
  }

  /**
   * Getter for maxrows.
   */
  public int getMaxRows() {
    return maxrows;
  }

  /**
   * Setter for maxrows.
   * 
   * @param maxrows value to set maxrows to
   */
  protected void setMaxRows(int maxrows) {
    this.maxrows = maxrows;
  }

  /**
   * Getter for error results.
   * @param errMessage message to be returned
   * @return the error message as part of a RequestResult
   */
  public RequestResult getErrorResult(String errMessage) {
    List<String> text = new ArrayList<String>();
    text.add(errMessage);
    TextResult result = new TextResult(text);
    result.setError(true);
    return result;
  }

  /**
   * Retrieve a collection of metadata.
   * 
   * @param md the pattern to match (integers < 0 & null strings are ignored)
   * @param cm is the name of the columns table coulmns_menu?
   * @return the desired metadata (null if an error occurred)
   */
  private List<MetaDatum> getMatchingMetaData(MetaDatum md, boolean cm) throws Exception {
    winston.useRootDatabase();
    String sql = "SELECT MD.cmid, MD.sid, -1, -1, MD.name, MD.value, CH.code, \"\", \"\" "
               + "FROM channelmetadata as MD, channels as CH WHERE MD.sid=CH.sid";

    if (md.chName != null) {
      sql += " AND CH.code='" + md.chName + "'";
    } else if (md.cid >= 0) {
      sql += " AND MD.cid=" + md.cid;
    }
    
    if (md.name != null) {
      sql += " AND MD.name=" + md.name;
    }
    
    if (md.name != null) { 
      sql += " AND MD.value=" + md.value;
    }

    PreparedStatement ps = winston.getPreparedStatement(sql);
    ResultSet rs = ps.executeQuery();
    List<MetaDatum> result = new ArrayList<MetaDatum>();
    while (rs.next()) {
      md = new MetaDatum();
      md.cmid = rs.getInt(1);
      md.cid = rs.getInt(2);
      md.colid = rs.getInt(3);
      md.rid = rs.getInt(4);
      md.name = rs.getString(5);
      md.value = rs.getString(6);
      md.chName = rs.getString(7);
      md.colName = rs.getString(8);
      md.rkName = rs.getString(9);
      result.add(md);
    }
    rs.close();
    return result;
  }

  /**
   * Retrieve a collection of supplementary data.
   * 
   * @param sd the pattern to match (integers < 0 & null strings are ignored)
   * @param cm = "is the name of the columns table coulmns_menu?"
   * @return the desired supplementary data (null if an error occurred)
   */
  private List<SuppDatum> getMatchingSuppData(SuppDatum sd, boolean cm) {
    winston.useRootDatabase();
    String sql = "SELECT SD.sdid, -1, SD.st, SD.et, SD.sd_short, SD.sd, CH.code, \"\", \"\", "
               + "ST.supp_data_type, ST.supp_color, SX.cid, -1, -1, ST.draw_line "
               + "FROM supp_data as SD, channels as CH, supp_data_type as ST, "
               + "supp_data_xref as SX WHERE SD.et >= " + sd.st + " AND SD.st <= " + sd.et
               + " AND SD.sdid=SX.sdid AND SD.sdtypeid=ST.sdtypeid AND SX.cid=CH.sid";

    if (sd.chName != null) {
      if (sd.cid < 0) {
        sql += " AND CH.code='" + sd.chName + "'";
      } else {
        sql += " AND CH.sid IN (" + sd.chName + ")";
      }
    } else if (sd.cid >= 0) {
      sql += " AND CH.sid=" + sd.cid;
    }

    if (sd.name != null) {
      sql += " AND SD.sd_short=" + sd.name;
    }
    if (sd.value != null) {
      sql += " AND SD.sd=" + sd.value;
    }

    String typeFilter = null;
    if (sd.typeName != null) {
      if (sd.typeName.length() == 0) {
        ;
      } else if (sd.tid == -1) {
        typeFilter = "ST.supp_data_type='" + sd.typeName + "'";
      } else {
        typeFilter = "ST.sdtypeid IN (" + sd.typeName + ")";
      }
    } else if (sd.tid >= 0) {
      typeFilter = "SD.sdtypeid=" + sd.tid;
    }

    if (sd.dl == -1) {
      if (typeFilter != null) {
        sql += " AND " + typeFilter;
      }
    } else if (sd.dl < 2) {
      if (typeFilter != null) {
        sql += " AND " + typeFilter;
      }
      sql += " AND ST.dl='" + sd.dl;
    } else if (typeFilter != null) {
      sql += " AND (" + typeFilter + " OR ST.draw_line='0')";
    } else {
      sql += " AND ST.draw_line='0'";
    }


    PreparedStatement ps = winston.getPreparedStatement(sql);
    ResultSet rs;
    List<SuppDatum> result = new ArrayList<SuppDatum>();

    try {
      rs = ps.executeQuery();
      while (rs.next()) {
        result.add(new SuppDatum(rs));
      }
      rs.close();
    } catch (SQLException e) {
      // Do nothing
    }

    return result;
  }

  /**
   * Insert a piece of metadata.
   * 
   * @param md the MetaDatum to be added
   */
  public void insertMetaDatum(MetaDatum md) throws Exception {
    winston.useRootDatabase();

    String sql = "INSERT INTO channelmetadata (sid,name,value) VALUES (" + md.cid + ",\"" + md.name
        + "\",\"" + md.value + "\");";

    PreparedStatement ps = winston.getPreparedStatement(sql);

    ps.execute();
  }

  /**
   * Update a piece of metadata.
   * 
   * @param md the MetaDatum to be updated
   */
  public void updateMetaDatum(MetaDatum md) throws Exception {
    winston.useRootDatabase();

    String sql = "UPDATE channelmetadata SET sid='" + md.cid + "', name='" + md.name + "', value='"
        + md.value + "' WHERE cmid='" + md.cmid + "'";

    PreparedStatement ps = winston.getPreparedStatement(sql);

    ps.execute();
  }

  public Channels getChannels() {
    return channels;
  }

  /**
   * Retrieve the collection of supplementary data types.
   * 
   * @return the desired supplementary data types
   */
  public List<SuppDatum> getSuppDataTypes() throws Exception {

    List<SuppDatum> types = new ArrayList<SuppDatum>();
    winston.useRootDatabase();
    String sql = "SELECT * FROM supp_data_type ORDER BY supp_data_type";
    PreparedStatement ps = winston.getPreparedStatement(sql);
    ResultSet rs = ps.executeQuery();
    while (rs.next()) {
      SuppDatum sd = new SuppDatum(0.0, 0.0, -1, -1, -1, rs.getInt(1));
      sd.typeName = rs.getString(2);
      sd.color = rs.getString(3);
      sd.dl = rs.getInt(4);
      types.add(sd);
    }
    rs.close();
    return types;
  }

  /**
   * Insert a piece of supplemental data.
   * 
   * @param sd the SuppDatum to be added
   */
  public int insertSuppDatum(SuppDatum sd) throws Exception {
    String sql;
    PreparedStatement ps;
    ResultSet rs;

    try {
      winston.useRootDatabase();

      sql = "INSERT INTO supp_data (sdtypeid,st,et,sd_short,sd) VALUES (" + sd.tid + "," + sd.st
          + "," + sd.et + ",\"" + sd.name + "\",\"" + sd.value + "\")";

      ps = winston.getPreparedStatement(sql);

      ps.execute();

      rs = ps.getGeneratedKeys();

      rs.next();

      return rs.getInt(1);
    } catch (SQLException e) {
      if (!e.getSQLState().equals("23000")) {
        throw e;
      }
    }
    sql = "SELECT sdid FROM supp_data WHERE sdtypeid=" + sd.tid + " AND st=" + sd.st + " AND et="
        + sd.et + " AND sd_short='" + sd.name + "'";
    ps = winston.getPreparedStatement(sql);

    // int sdid = 0;

    rs = ps.executeQuery();

    rs.next();

    return -rs.getInt(1);
  }

  /**
   * Update a piece of supplemental data.
   * 
   * @param sd the SuppDatum to be added
   * @return ID of the record, 0 if failed
   */
  public int updateSuppDatum(SuppDatum sd) throws Exception {
    String sql;
    PreparedStatement ps;
    // ResultSet rs;
    winston.useRootDatabase();

    sql = "UPDATE supp_data SET sdtypeid='" + sd.tid + "',st='" + sd.st + "',et='" + sd.et
        + "',sd_short='" + sd.name + "',sd='" + sd.value + "' WHERE sdid='" + sd.sdid + "'";

    ps = winston.getPreparedStatement(sql);

    ps.execute();

    return sd.sdid;
  }

  /**
   * Insert a supplemental data xref.
   * 
   * @param sd the SuppDatum xref to be added
   * @return insertion was successful
   */
  public boolean insertSuppDatumXref(SuppDatum sd) throws Exception {
    String sql;
    PreparedStatement ps;
    // ResultSet rs;
    try {
      winston.useRootDatabase();

      sql = "INSERT INTO supp_data_xref (sdid, cid) VALUES (" + sd.sdid + "," + sd.cid + ");";

      ps = winston.getPreparedStatement(sql);

      ps.execute();
    } catch (SQLException e) {
      if (!e.getSQLState().equals("23000")) {
        throw e;
      }
      return false;
    }
    return true;
  }

  /**
   * Insert a supplemental datatype.
   * 
   * @param sd the datatype to be added
   * @return ID of the datatype, -ID if already present, 0 if failed
   */
  public int insertSuppDataType(SuppDatum sd) throws Exception {
    String sql;
    PreparedStatement ps;
    ResultSet rs;
    try {
      sql = "INSERT INTO supp_data_type (supp_data_type,supp_color,draw_line) VALUES (" + "\""
          + sd.typeName + "\",\"" + sd.color + "\"," + sd.dl + ");";

      ps = winston.getPreparedStatement(sql);

      ps.execute();

      rs = ps.getGeneratedKeys();

      rs.next();

      return rs.getInt(1);
    } catch (SQLException e) {
      if (!e.getSQLState().equals("23000")) {
        throw e;
      }
    }
    sql = "SELECT sdid FROM supp_data WHERE sdtypeid=" + sd.tid + " AND st=" + sd.st + " AND et="
        + sd.et + " AND sd_short='" + sd.name + "'";
    ps = winston.getPreparedStatement(sql);
    rs = ps.executeQuery();
    rs.next();
    return -rs.getInt(1);
  }

  /**
   * Process a getData request for supplementary data from this datasource.
   * 
   * @param params parameters for this request
   * @param cm = "is the name of the columns table coulmns_menu?"
   * @return RequestResult the desired supplementary data (null if an error occurred)
   */

  protected RequestResult getSuppData(Map<String, String> params, boolean cm) {
    double st;
    double et;
    String arg = null;
    SuppDatum suppDatum;

    String tz = params.get("tz");
    if (tz == null || tz == "") {
      tz = "UTC";
    }
    SimpleDateFormat df = new SimpleDateFormat("yyyyMMddHHmmssSSS");
    df.setTimeZone(TimeZone.getTimeZone(tz));

    try {
      arg = params.get("st");
      st = J2kSec.fromDate(df.parse(arg));
      arg = params.get("et");
      if (arg == null || arg == "") {
        et = Double.MAX_VALUE;
      } else {
        et = J2kSec.fromDate(df.parse(arg));
      }
    } catch (Exception e) {
      return getErrorResult("Illegal time string: " + arg + ", " + e);
    }

    arg = params.get("byID");
    if (arg != null && arg.equals("true")) {
      arg = params.get("et");
      if (arg == null || arg.equals("")) {
        et = Double.MAX_VALUE;
      } else {
        et = Double.parseDouble(arg);
      }
      arg = params.get("type");
      int tid;
      if (arg == null || arg.equals("")) {
        tid = -1;
      } else {
        tid = Integer.parseInt(arg);
      }
      
      int cid = Integer.parseInt(params.get("ch"));
      suppDatum = new SuppDatum(st, et, cid, -1, -1, tid);
    } else {
      String chName = params.get("ch");
      String typeName = params.get("type");
      suppDatum = new SuppDatum(st, et, chName, null, null, typeName);
    }
    
    List<SuppDatum> data = getMatchingSuppData(suppDatum, cm);
    if (data != null) {
      List<String> result = new ArrayList<String>();
      for (SuppDatum sd : data) {
        result.add(String.format(
            "%d,%1.3f,%1.3f,%d,%d,%d,%d,%d,\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\"",
            sd.sdid, sd.st, sd.et, sd.cid, sd.tid, sd.colid, sd.rid, sd.dl, sd.name,
            sd.value.replace('\n', Character.MIN_VALUE), sd.chName, sd.typeName, sd.colName,
            sd.rkName, sd.color));
      }
      return new TextResult(result);
    }
    return null;
  }

  /**
   * Get supp data types list in format 'sdtypeid"draw_line"name"color' from database.
   * 
   * @param drawOnly ="yield only types that can be drawn"
   * @return List of Strings with " separated values
   */
  public RequestResult getSuppTypes(boolean drawOnly) throws Exception {
    winston.useRootDatabase();
    List<String> result = new ArrayList<String>();

    String sql;
    // PreparedStatement ps;
    ResultSet rs;
    sql = "SELECT * FROM supp_data_type";
    if (drawOnly) {
      sql = sql + " WHERE draw_line=1";
    }
    rs = winston.getPreparedStatement(sql + " ORDER BY supp_data_type").executeQuery();
    while (rs.next()) {
      result.add(String.format("%d\"%d\"%s\"%s", rs.getInt(1), rs.getInt(4), rs.getString(2),
          rs.getString(3)));
    }
    rs.close();

    return new TextResult(result);
  }
}
