package gov.usgs.volcanoes.vdx.in.gps;

import gov.usgs.volcanoes.core.configfile.ConfigFile;
import gov.usgs.volcanoes.core.util.ResourceReader;
import gov.usgs.volcanoes.vdx.data.gps.SQLGpsDataSource;

/**
 * Import benchmarks from file.
 *
 * @author Dan Cervelli
 */
public class ImportSum {

  private SQLGpsDataSource dataSource;

  /**
   * Constructor.
   *
   * @param prefix vdx prefix
   * @param name vdx name
   */
  public ImportSum(String prefix, String name) {
    dataSource = new SQLGpsDataSource();
    ConfigFile params = new ConfigFile();
    params.put("vdx.driver", "com.mysql.jdbc.Driver");
    params.put("vdx.url", "jdbc:mysql://localhost/?user=vdx&password=vdx");
    params.put("vdx.prefix", prefix);
    params.put("vdx.name", name);
    // TODO: work out new initialization
    // dataSource.initialize(params);
  }

  /**
   * Import benchmarks file.
   *
   * @param fn file name
   */
  public void importSum(String fn) {

    ResourceReader rr;

    rr = ResourceReader.getResourceReader(fn);
    if (rr == null) {
      return;
    }

    String[] ss;
    String bm;
    ss = rr.nextLine().split("\t");
    bm = ss[1].toUpperCase();

    double dlat;
    double sign;
    ss = rr.nextLine().split("\t");
    dlat = Double.parseDouble(ss[1].trim());
    sign = 1;
    if (dlat < 0) {
      sign = -1;
    }
    dlat = Math.abs(dlat);

    double lat;
    lat = dlat + Double.parseDouble(ss[2].trim()) / 60 + Double.parseDouble(ss[3].trim()) / 3600;
    lat *= sign;

    double dlon;
    ss = rr.nextLine().split("\t");
    dlon = Double.parseDouble(ss[1].trim());
    sign = 1;
    if (dlon < 0) {
      sign = -1;
    }
    dlon = Math.abs(dlon);

    double lon;
    lon = dlon + Double.parseDouble(ss[2].trim()) / 60 + Double.parseDouble(ss[3].trim()) / 3600;
    lon *= sign;

    double height;
    ss = rr.nextLine().split("\t");
    height = Double.parseDouble(ss[1].trim());

    dataSource.createChannel(bm, bm, lon, lat, height, 1);
    System.out.printf("%s %f %f %f\n", bm, lon, lat, height);

    rr.close();
  }

  /**
   * Main method.
   *
   * @param args command line args
   */
  public static void main(String[] args) {
    ImportSum is = new ImportSum(args[0], args[1]);
    for (int i = 2; i < args.length; i++) {
      is.importSum(args[i]);
    }
  }
}
