package gov.usgs.volcanoes.vdx.data.lightning;

import gov.usgs.volcanoes.core.configfile.ConfigFile;
import gov.usgs.volcanoes.core.util.UtilException;
import gov.usgs.volcanoes.vdx.data.DataSource;
import gov.usgs.volcanoes.vdx.data.SQLDataSource;
import gov.usgs.volcanoes.vdx.server.BinaryResult;
import gov.usgs.volcanoes.vdx.server.RequestResult;
import gov.usgs.volcanoes.vdx.server.TextResult;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * SQL Data Source for lightning Data. Modeled after SQLHypocenterSource
 *
 * @author Tom Parker
 * @author Bill Tollett
 */
public class SQLLightningDataSource extends SQLDataSource implements DataSource {

  private static final Logger LOGGER = LoggerFactory.getLogger(SQLLightningDataSource.class);

  public static final String DATABASE_NAME = "lightning";
  public static final boolean channels = false;
  public static final boolean translations = false;
  public static final boolean channelTypes = false;
  public static final boolean ranks = true;
  public static final boolean columns = false;
  public static final boolean menuColumns = false;

  /**
   * Get database type, generic in this case.
   *
   * @return type
   */
  public String getType() {
    return DATABASE_NAME;
  }

  /**
   * Get channels flag.
   *
   * @return channels flag
   */
  public boolean getChannelsFlag() {
    return channels;
  }

  /**
   * Get translations flag.
   *
   * @return translations flag
   */
  public boolean getTranslationsFlag() {
    return translations;
  }

  /**
   * Get channel types flag.
   *
   * @return channel types flag
   */
  public boolean getChannelTypesFlag() {
    return channelTypes;
  }

  /**
   * Get ranks flag.
   *
   * @return ranks flag
   */
  public boolean getRanksFlag() {
    return ranks;
  }

  /**
   * Get columns flag.
   *
   * @return columns flag
   */
  public boolean getColumnsFlag() {
    return columns;
  }

  /**
   * Get menu columns flag.
   *
   * @return menu columns flag
   */
  public boolean getMenuColumnsFlag() {
    return menuColumns;
  }

  /**
   * Initialize data source.
   *
   * @param params config file
   */
  public void initialize(ConfigFile params) {
    defaultInitialize(params);
    if (!databaseExists()) {
      createDatabase();
    }
  }

  /**
   * De-Initialize data source.
   */
  public void disconnect() {
    defaultDisconnect();
  }

  /**
   * Get flag if database exists.
   *
   * @return true if database exists, false otherwise
   */
  public boolean databaseExists() {
    return defaultDatabaseExists();
  }

  /**
   * Create hypocenters database.
   *
   * @return true if successful, false otherwise
   */
  public boolean createDatabase() {

    try {
      defaultCreateDatabase(channels, translations, channelTypes, ranks, columns, menuColumns);

      // setup the database for running some statements
      database.useDatabase(dbName);
      st = database.getStatement();

      // create the hypocenters table
      sql = "CREATE TABLE strokes (j2ksec DOUBLE NOT NULL, ";
      sql += "   lat DOUBLE NOT NULL, lon DOUBLE NOT NULL";
      sql += "stationsDetected INT not null, residual DOUBLE NOT NULL";
      sql += " PRIMARY KEY(j2ksec, lat, lon), KEY index_j2ksec (j2ksec))";
      st.execute(sql);

      LOGGER.info("SQLLightningDataSource.createDatabase({}_{}) succeeded.",
          database.getDatabasePrefix(), dbName);

      return true;

    } catch (Exception e) {
      LOGGER.error("SQLLightningDataSource.createDatabase({}_{}) failed.",
          database.getDatabasePrefix(), dbName, e);
    }
    return false;
  }

  /**
   * Getter for data. Search value of 'action' parameter and retrieve corresponding data.
   *
   * @param params command to execute, map of parameter-value pairs.
   * @return request result
   */
  public RequestResult getData(Map<String, String> params) {

    String action = params.get("action");

    if (action == null) {
      return null;

    } else if (action.equals("ranks")) {
      return new TextResult(defaultGetRanks());

    } else if (action.equals("data")) {
      int rid = Integer.parseInt(params.get("rk"));
      double st = Double.parseDouble(params.get("st"));
      double et = Double.parseDouble(params.get("et"));
      double west = Double.parseDouble(params.get("west"));
      double east = Double.parseDouble(params.get("east"));
      double south = Double.parseDouble(params.get("south"));
      double north = Double.parseDouble(params.get("north"));
      StrokeList data = null;
      try {
        data = getStrokeData(rid, st, et, west, east, south, north, getMaxRows());
      } catch (UtilException e) {
        return getErrorResult(e.getMessage());
      }
      if (data != null) {
        return new BinaryResult(data);
      }
    } else if (action.equals("metadata")) {
      return getMetaData(params, false);
    }
    return null;
  }

  /**
   * Get Hypocenter data.
   *
   * @param rid rank id
   * @param st start time
   * @param et end time
   * @param west west boundary
   * @param east east boundary
   * @param south south boundary
   * @param north north boundary
   * @param maxrows maximum nbr of rows returned
   * @return list of hypocenter data
   */
  public StrokeList getStrokeData(int rid, double st, double et, double west, double east,
      double south, double north,
      int maxrows) throws UtilException {

    List<Stroke> pts = new ArrayList<Stroke>();
    StrokeList result = null;

    try {

      database.useDatabase(dbName);

      // calculate the num of rows to limit the query to
      int tempmaxrows;
      if (rid != 0) {
        tempmaxrows = maxrows;
      } else {
        tempmaxrows = maxrows * defaultGetNumberOfRanks();
      }

      sqlCount = "SELECT COUNT(*) FROM (SELECT 1 ";

      // build the sql
      sql = "SELECT a.j2ksec, a.rid, a.lat, a.lon, a.stationsDetected, a.residual";
      sql += " FROM   strokes a, ranks c ";
      sql += " WHERE  a.rid = c.rid ";
      sql += " AND    a.j2ksec  >= ? AND a.j2ksec  <= ? ";

      if (west <= east) {
        sql += " AND a.lon >= ? AND a.lon <= ? ";
      } else {
        // wrap around date line
        sql += " AND (a.lon >= ? OR a.lon <= ?) ";
      }

      sql += " AND    a.lat     >= ? AND a.lat     <= ? ";

      // BEST AVAILABLE DATA query
      if (ranks && rid != 0) {
        sql += " AND    c.rid  = ? ";
      }

      sql += " ORDER BY a.j2ksec ASC";

      if (ranks && rid == 0) {
        sql = sql + ", c.rank DESC";
      }

      if (maxrows != 0) {
        sql += " LIMIT " + (tempmaxrows + 1);

        // If the dataset has a maxrows paramater, check that the number
        // of requested rows doesn't
        // exceed that number prior to running the full query. This can
        // save a decent amount of time
        // for large queries.
        ps = database
            .getPreparedStatement(sqlCount + sql.substring(sql.indexOf("FROM")) + ") as T");
        ps.setDouble(1, st);
        ps.setDouble(2, et);
        ps.setDouble(3, west);
        ps.setDouble(4, east);
        ps.setDouble(5, south);
        ps.setDouble(6, north);
        if (ranks && rid != 0) {
          ps.setInt(7, rid);
        }
        rs = ps.executeQuery();
        if (rs.next() && rs.getInt(1) > tempmaxrows) {
          throw new UtilException(
              "Max rows (" + maxrows + " rows) for data source '" + vdxName + "' exceeded.");
        }
      }

      ps = database.getPreparedStatement(sql);
      ps.setDouble(1, st);
      ps.setDouble(2, et);
      ps.setDouble(3, west);
      ps.setDouble(4, east);
      ps.setDouble(5, south);
      ps.setDouble(6, north);
      if (ranks && rid != 0) {
        ps.setInt(7, rid);
      }

      rs = ps.executeQuery();

      double j2ksec;
      double lat;
      double lon;
      double residual;
      int stationsDetected;

      // loop through each result and add to the list
      while (rs.next()) {

        // if this is a new eid, then save this data, as it contains the
        // highest rank

        // these will never be null
        j2ksec = getDoubleNullCheck(rs, 1);
        rid = rs.getInt(2);
        lat = getDoubleNullCheck(rs, 3);
        lon = getDoubleNullCheck(rs, 4);
        stationsDetected = getIntNullCheck(rs, 5);
        residual = getDoubleNullCheck(rs, 6);

        pts.add(new Stroke(j2ksec, rid, lat, lon, stationsDetected, residual));
      }
      rs.close();

      if (pts.size() > 0) {
        Collections.sort(pts, new Comparator<Stroke>() {
          public int compare(Stroke h1, Stroke h2) {
            if (h1.j2ksec > h2.j2ksec) {
              return 1;
            } else {
              return -1;
            }
          }
        });
      }

    } catch (SQLException e) {
      LOGGER.error("SQLLightningDataSource.getHypocenterData() failed.", e);
    }
    result = new StrokeList(pts);
    return result;
  }

  /**
   * Insert data.
   *
   * @param hc Hypocenter
   */
  public int insertStrike(Stroke hc) {

    int result = -1;

    try {
      database.useDatabase(dbName);
      sql = "REPLACE INTO strokes ";
      sql += "       (j2ksec, rid, lat, lon) ";
      sql += "VALUES (?,?,round(?, 4),round(?, 4))";
      ps = database.getPreparedStatement(sql);

      // required fields
      ps.setDouble(1, hc.j2ksec);
      ps.setInt(3, hc.rid);
      ps.setDouble(4, hc.lat);
      ps.setDouble(5, hc.lon);

      result = ps.executeUpdate();

    } catch (Exception e) {
      LOGGER.error("SQLLightningDataSource.insertHypocenter() failed.", e);
    }
    return result;
  }
}
