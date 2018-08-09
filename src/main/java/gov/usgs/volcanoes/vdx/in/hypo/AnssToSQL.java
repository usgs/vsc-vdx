package gov.usgs.volcanoes.vdx.in.hypo;

import gov.usgs.volcanoes.core.legacy.math.proj.GeographicFilter;
import gov.usgs.volcanoes.core.time.J2kSec;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Scanner;
import java.util.TimeZone;

/**
 * ANSS to SQL Reads a ANSS hypocenter files and returns SQL statements suitable for insertion into
 * a Valve 3 database to the stdout. See: http://www.ncedc.org/ftp/pub/doc/cat5/cnss.catalog.5 for a
 * description of the hypocenter format file.
 *
 * @author Peter Cervelli
 */
public class AnssToSQL {

  private static final String INPUT_DATE_FORMAT = "yyyyMMddHHmmss";
  private static int rankID = 1;
  private static String clause = "REPLACE";
  private static GeographicFilter GF = new GeographicFilter();
  private static boolean web = false;

  private static double parseTime(String line) {

    String timeStamp = line.substring(5, 19);
    Double milliseconds = Double.parseDouble(line.substring(20, 24)) / 10000;
    if (timeStamp.trim().length() == 0) {
      return 0;
    } else {
      SimpleDateFormat format = new SimpleDateFormat(INPUT_DATE_FORMAT);
      format.setTimeZone(TimeZone.getTimeZone("UTC"));
      try {
        return J2kSec.fromDate(format.parse(timeStamp.trim())) + milliseconds;
      } catch (ParseException e) {
        e.printStackTrace();
        return 0;
      }
    }

  }

  private static String parse(String line, int i, int j, String defaultString) {
    line = line.substring(i, j).trim();
    if (line.length() == 0) {
      return defaultString;
    } else {
      return line;
    }
  }


  private static String parse(String line, int i, int j) {
    return parse(line, i, j, "0");
  }


  private static void printLine(String line, String preamble) {

    System.out.printf("%s(%.2f,\"%s\",%d,%s,%s,-%s,%s,%s,%s,%s,%s,%s,%s,%s,\"%s\")",
        preamble,
        parseTime(line),  // Time
        parse(line, 111, 123, "NONE"), //Event ID
        rankID, // Rank ID
        parse(line, 24, 33), // Latitude
        parse(line, 33, 43), // Longitude
        parse(line, 43, 51), // Depth
        parse(line, 129, 134), // Preferred Magnitude
        parse(line, 56, 60), // Number of Phases
        parse(line, 60, 63), // Azimuthal Gap
        parse(line, 63, 73), // Distance to nearest station
        parse(line, 73, 80), // RMS error
        parse(line, 56, 60), // nstimes
        parse(line, 87, 94), // Horizontal Error
        parse(line, 94, 101), //Vertical Error
        parse(line, 134, 135) // Magnitude Type
    );

  }

  private static void printLineWeb(String line, String preamble) {

    System.out.printf("%s(\"%s\",%s,%s,%s,%s,\"%s%s\")",
        preamble,
        J2kSec.toDateString(parseTime(line)),  // Time
        parse(line, 24, 33), // Latitude
        parse(line, 33, 43), // Longitude
        parse(line, 43, 51), // Depth
        parse(line, 129, 134), // Preferred Magnitude
        parse(line, 53, 55).toLowerCase(), // Network
        parse(line, 111, 123, "") //Event ID
    );

  }

  private static int parseArguments(String[] args) {

    int c = 0;

    while (args[c].startsWith("--")) {
      String key = args[c].split("=")[0].substring(2);
      if (key.toLowerCase().startsWith("c")) {
        String value = args[c].split("=")[1];
        GF.addCircle(Double.parseDouble(value.split(",")[0]),
            Double.parseDouble(value.split(",")[1]), Double.parseDouble(value.split(",")[2]));
      } else if (key.toLowerCase().startsWith("b")) {
        String value = args[c].split("=")[1];
        GF.addBox(Double.parseDouble(value.split(",")[0]), Double.parseDouble(value.split(",")[1]),
            Double.parseDouble(value.split(",")[2]), Double.parseDouble(value.split(",")[3]));
      } else if (key.toLowerCase().startsWith("r")) {
        String value = args[c].split("=")[1];
        rankID = Integer.parseInt(value);
      } else if (key.toLowerCase().startsWith("i")) {
        clause = "INSERT IGNORE";
      } else if (key.toLowerCase().startsWith("w")) {
        web = true;
      }
      c++;
    }

    return c;

  }

  private static String readFile(String fileName) throws IOException {

    FileInputStream fin = null;
    FileChannel fchan = null;
    long fsize;
    ByteBuffer mbuf;

    final StringBuilder builder = new StringBuilder();

    try {
      fin = new FileInputStream(fileName);
      fchan = fin.getChannel();
      fsize = fchan.size();
      mbuf = ByteBuffer.allocate((int) fsize);
      fchan.read(mbuf);
      mbuf.rewind();
      for (int i = 0; i < fsize; i++) {
        byte token = mbuf.get();
        if (token == 13) {
          token = 32;
        }
        builder.append((char) token);
      }
      fchan.close();
      fin.close();
    } catch (final IOException exc) {
      System.err.println(exc);
      System.exit(1);
    } finally {
      if (fchan != null) {
        fchan.close();
      }
      if (fin != null) {
        fin.close();
      }
    }

    return builder.toString();
  }

  /**
   * Main method.
   *
   * @param args string[]
   */
  public static void main(String[] args) {

    String line;
    double longitude;
    double latitude;
    final long start_time = System.currentTimeMillis();

    if (args.length == 0) {
      System.out.println("Usage: AnssToSQL options ANSSfile ...");
      System.out.println("");
      System.out.println("Reads ANSS (cnss) hypocenter files and writes SQL statements to the");
      System.out.println("standard output suitable for insertion into a VALVE GPS database.");
      System.out.println("Multiple ANSS (cnss) files may be given.");
      System.out.println("");
      System.out.println("Arguments:");
      System.out.println("");
      System.out.println("  --rankID=<rank>");
      System.out.println("     Assigns the rankID of the inserted solution. If omitted, the");
      System.out.println("     default is 1.");
      System.out.println("");
      System.out.println("  --box=<minLon>,<maxLon>,<minLat>,<maxLat>");
      System.out.println("     Defines a geographic box filter. Stations within this box pass");
      System.out.println("     through the filter. Longitude and latitude are given in decimal");
      System.out.println("     degrees.");
      System.out.println("");
      System.out.println("  --circle=<radius>,<Lon>,<Lat>");
      System.out.println("     Defines a geographic circle filter. Stations within this circle");
      System.out.println("     pass throught the filter. Longitude and latitude are given in");
      System.out.println("     decimal degrees. Radius is given in kilometers.");
      System.out.println("");
      System.out.println("  --ignore");
      System.out.println("     SQL statements constructed so that existing hypocenters are");
      System.out.println("     not overwritten");
      System.out.println("");
      System.out.println("Notes:");
      System.out.println("");
      System.out.println("  Multiple geographic filters may be defined. Only one station filter");
      System.out.println("  may be defined.");
      System.out.println(" ");
      System.out.println("  The 'circle' is actually a spherical cap, meaning that the");
      System.out.println("  radius value is a great circle distance.  Distance is computed on a");
      System.out.println("  sphere of radius = 6378137 meters. Likewise, the 'box' is");
      System.out.println("  really a latitude-longitude rectangle on a sphere.");
      System.out.println("");
      System.out.println("  The arguments can be given in any order. White space separates the");
      System.out.println("  argument.");
      System.out.println("");
      System.out.println("Version 1.0, November 20, 2011, report bugs to: pcervelli@usgs.gov");
      System.out.println("");
      return;
    }

    int c = parseArguments(args);

    for (int i = c; i < args.length; i++) {

      boolean first = true;
      Scanner s = null;

      try {
        s = new Scanner(readFile(args[i]));
      } catch (IOException e) {
        System.err.printf("Could not open file: %s%n", args[i]);
        continue;

      }

      System.err.printf("Reading file: %s%n", args[i]);

      int j = 0;
      while (s.hasNextLine()) {

        line = s.nextLine();
        j++;

        try {
          latitude = Double.parseDouble(line.substring(24, 33));
          longitude = Double.parseDouble(line.substring(33, 43));

          if (GF.test(longitude, latitude)) {
            if (first) {
              if (web) {
                System.out.printf(
                    "%s INTO hypocenter (hypocenter_date_time,latitude,longitude,"
                    + "depth,magnitude,event_id) VALUES%n", clause);
                printLineWeb(line, "");
              } else {
                System.out.printf(
                    "%s INTO hypocenters (j2ksec,eid,rid,lat,lon,depth,prefmag,"
                    + " nphases,azgap,dmin,rms,nstimes,herr,verr,magtype) VALUES%n", clause);
                printLine(line, "");
              }

              first = false;
              continue;
            }
            if (web) {
              printLineWeb(line, ",\n");
            } else {
              printLine(line, ",\n");
            }

          }
        } catch (Exception e) {
          e.printStackTrace();
          System.err.printf("Could not read line %d in file %s.%n", j, args[i]);
        }
      }
      if (!first) {
        System.out.printf(";%n");
      }
      s.close();

    }

    System.err.printf("\nElapsed Time: %d ms%n", System.currentTimeMillis() - start_time);

  }
}
