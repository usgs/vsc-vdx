package gov.usgs.volcanoes.vdx.in.hypo;

import gov.usgs.volcanoes.core.legacy.Arguments;
import gov.usgs.volcanoes.core.time.J2kSec;
import gov.usgs.volcanoes.core.util.ResourceReader;
import gov.usgs.volcanoes.vdx.data.hypo.Hypocenter;
import gov.usgs.volcanoes.vdx.data.hypo.SQLHypocenterDataSource;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;

/**
 * Class for importing hypo71 format catalog files.
 *
 * @author Tom Parker
 */
public class ImportH71 extends Importer {

  private SimpleDateFormat dateIn;

  /**
   * Constructor.
   *
   * @param ds data source to import in
   */
  public ImportH71(SQLHypocenterDataSource ds) {
    super(ds);
    dateIn = new SimpleDateFormat("yyyyMMddHHmmsss.SS");// ss.SS");
    dateIn.setTimeZone(TimeZone.getTimeZone("GMT"));
  }

  /**
   * Parse H71 file from url (resource locator or file name).
   *
   * @param resource resource identifier
   * @return Hypocenters list
   */
  public List<Hypocenter> importResource(String resource) {
    ResourceReader rr = ResourceReader.getResourceReader(resource);
    if (rr == null) {
      return null;
    }

    List<Hypocenter> hypos = new ArrayList<Hypocenter>();
    String s;
    int lines = 0;
    while ((s = rr.nextLine()) != null) {
      try {
        lines++;

        if (!s.substring(8, 9).equals(" ")) {
          throw new Exception("corrupt data at column 9");
        }

        // LAT
        double latdeg = Double.parseDouble(s.substring(19, 22).trim());
        double latmin = Double.parseDouble(s.substring(23, 28).trim());
        double lat = latdeg + latmin / 60.0d;
        char ns = s.charAt(22);
        if (ns == 'S') {
          lat *= -1;
        }

        // LON
        double londeg = Double.parseDouble(s.substring(28, 32).trim());
        char ew = s.charAt(32);
        double lonmin = Double.parseDouble(s.substring(33, 38).trim());
        double lon = londeg + lonmin / 60.0d;
        if (ew != 'W') {
          lon *= -1;
        }

        // DEPTH
        double depth = Double.parseDouble(s.substring(38, 45).trim());

        // MAGNITUDE
        double mag = Double.parseDouble(s.substring(47, 52).trim());

        if (!s.substring(45, 46).equals(" ")) {
          throw new Exception("corrupt data at column 46");
        }

        String year = s.substring(0, 4);
        String monthDay = s.substring(4, 8);
        String hourMin = s.substring(9, 13);
        String sec = s.substring(13, 19).trim();
        Date date = dateIn.parse(year + monthDay + hourMin + sec);
        double j2ksec = J2kSec.fromDate(date);
        System.out
            .println("HC: " + j2ksec + " : " + lon + " : " + lat + " : " + depth + " : " + mag);
        Hypocenter hc = new Hypocenter(j2ksec, 0, lat, lon, depth, mag);
        hypos.add(hc);
      } catch (Exception e) {
        System.err.println("Line " + lines + ": " + e.getMessage());
      }
    }
    rr.close();
    return hypos;
  }

  /**
   * Main method. Initialize data source using command line arguments and make import. Syntax is:
   * "[importer] -c [vdx config] -n [database name] files..."
   *
   * @param as command line args
   */
  public static void main(String[] as) {
    Arguments args = new Arguments(as, flags, keys);
    SQLHypocenterDataSource ds = Importer.getDataSource(args);
    process(args, new ImportH71(ds));
  }
}
