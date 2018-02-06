package gov.usgs.volcanoes.vdx.in.generic.variable;

import gov.usgs.volcanoes.core.configfile.ConfigFile;
import gov.usgs.volcanoes.core.legacy.Arguments;
import gov.usgs.volcanoes.core.util.ResourceReader;
import gov.usgs.volcanoes.vdx.data.generic.variable.DataType;
import gov.usgs.volcanoes.vdx.data.generic.variable.SQLGenericVariableDataSource;
import gov.usgs.volcanoes.vdx.data.generic.variable.Station;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TimeZone;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Import NWIS data from url.
 *
 * @author Tom Parker
 * @author Bill Tollett
 */
public class ImportNwis {

  private static final String CONFIG_FILE = "NWIS.config";
  private static final Logger LOGGER = LoggerFactory.getLogger(ImportNwis.class);
  private List<Station> stations;
  private SQLGenericVariableDataSource dataSource;
  private ConfigFile params;

  /**
   * Constructor.
   *
   * @param cf configuration file to specify data source to import in and data structure
   */
  public ImportNwis(String cf) {
    dataSource = new SQLGenericVariableDataSource();
    params = new ConfigFile(cf);

    // TODO: work out new initialization
    // dataSource.initialize(params);
    //dataSource.setName(params.getString("vdx.name"));

    // this is commented out until i figure out what to do with it  (LJA)
    // stations = dataSource.getStations();
  }

  /**
   * Import NWIS data from url ('url' parameter in the configuration).
   *
   * @param st station
   * @param period not used really
   */
  public void importWeb(Station st, int period) {
    if (!st.getActive()) {
      return;
    }

    List<DataType> dataTypes = new ArrayList<DataType>();
    String fn = params.getString("url") + "&period=" + period + "&site_no=" + st.getSiteNo();

    try {
      ResourceReader rr = ResourceReader.getResourceReader(fn);
      if (rr == null) {
        return;
      }
      LOGGER.info("importing: {}", fn);
      SimpleDateFormat dateIn;

      dateIn = new SimpleDateFormat("yyyy-MM-dd HH:mm");
      dateIn.setTimeZone(TimeZone.getTimeZone(st.getTz()));

      String s = rr.nextLine();
      Pattern p;
      Matcher m;

      // match header
      //p = Pattern.compile("^#\\s+\\*(\\d+)\\s+(\\d+)\\s+-\\s+(.*)$");
      p = Pattern.compile("^#\\s+(\\d+)\\s+(\\d+)\\s+(.*)$");
      Pattern p1 = Pattern.compile("^#.*$");
      while (s != null && p1.matcher(s).matches()) {
        m = p.matcher(s);
        if (m.matches()) {
          int dataType = Integer.parseInt(m.group(2));
          String name = m.group(3);
          dataTypes.add(new DataType(dataType, name));
        }

        s = rr.nextLine();
      }

      // parse column name row
      String[] ss = s.split("\t");
      for (int i = 0; i < dataTypes.size(); i++) {
        int index = i * 2 + 3;
        int id = Integer.parseInt(ss[index].substring(3));

        if (dataTypes.get(i).getId() != id) {
          DataType t;
          for (int j = i; j < dataTypes.size(); j++) {
            if (dataTypes.get(j).getId() == id) {
              t = dataTypes.get(i);
              dataTypes.set(i, dataTypes.get(j));
              dataTypes.set(j, t);
            }

          }
        }
        i++; // discard _cd column
      }

      s = rr.nextLine(); // discard collumn definition row

      // match records
      s = rr.nextLine();
      while (s != null) {
        ss = s.split("\t", -1);

        // assume midnight if no time given
        if (!ss[2].contains(" ")) {
          ss[2] += " 00:00";
        }

        Date date = dateIn.parse(ss[2]);
        for (int i = 0; i < dataTypes.size(); i++) {
          int index = i * 2 + 3;
          // ignore values that are empty or have embeded qualification codes
          if (ss[index].length() > 0 && ss[index].indexOf('_') == -1) {
            dataSource.insertRecord(date, st, dataTypes.get(i), Double.parseDouble(ss[index]));
          } else {
            System.out.println("skipping " + ss[index] + " idex: " + ss[index].indexOf('_'));
          }
          s = rr.nextLine();
        }
      }

      for (DataType dt : dataTypes) {
        dataSource.insertDataType(dt);
      }

      System.out.println();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  /**
   * Main method.
   *
   * @param as command line args
   */
  public static void main(String[] as) {
    String cf = CONFIG_FILE;
    int period = 1;
    Set<String> flags;
    Set<String> keys;

    flags = new HashSet<String>();
    keys = new HashSet<String>();
    keys.add("-c");
    keys.add("-h");
    keys.add("-p");

    Arguments args = new Arguments(as, flags, keys);

    if (args.contains("-h")) {
      System.err
          .println("java gov.usgs.volcanoes.vdx.data.gps.ImportNwis [-c configFile] [-p period]");
      System.exit(-1);
    }

    if (args.contains("-c")) {
      cf = args.get("-c");
    }

    if (args.contains("-p")) {
      period = Integer.parseInt(args.get("-p"));
    }

    ImportNwis in = new ImportNwis(cf);
    List<String> files = args.unused();

    for (Station station : in.stations) {
      in.importWeb(station, period);
    }

  }
}
