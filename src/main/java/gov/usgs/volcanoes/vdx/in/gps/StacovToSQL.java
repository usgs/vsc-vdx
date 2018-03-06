package gov.usgs.volcanoes.vdx.in.gps;

import gov.usgs.volcanoes.core.math.proj.Projection;

import java.awt.geom.Point2D;
import java.io.FileNotFoundException;
import java.text.ParseException;

/**
 * Stacov to SQL Reads a stacov files and returns SQL statements suitable for insertion into a Valve
 * 3 database to the stdout.
 * To do: Use the GeographicFilter class
 *
 * @author Peter Cervelli
 */
public class StacovToSQL {

  private static boolean circleFilter(double[] circleFilter, double longitude, double latitude) {

    if (circleFilter != null) {
      int k = 0;
      int ncircles = circleFilter.length / 3;
      boolean result = false;
      double radius;
      double phi;
      double lambda;

      while (!result & k < ncircles) {

        radius = circleFilter[k * 3] * 1000;
        phi = circleFilter[k * 3 + 1];
        lambda = circleFilter[k * 3 + 2];
        result = radius > Projection.distanceBetween(new Point2D.Double(phi, lambda),
            new Point2D.Double(longitude, latitude));
        k++;
      }
      return result;
    } else {
      return true;
    }

  }

  private static boolean boxFilter(double[] boxFilter, double longitude, double latitude) {

    if (boxFilter != null) {
      int k = 0;
      int nboxes = boxFilter.length / 4;
      boolean result = false;
      double minLon;
      double maxLon;
      double minLat;
      double maxLat;

      while (!result & k < nboxes) {
        minLon = boxFilter[k * 4];
        maxLon = boxFilter[k * 4 + 1];
        minLat = boxFilter[k * 4 + 2];
        maxLat = boxFilter[k * 4 + 3];
        result =
            longitude >= minLon & longitude <= maxLon & latitude >= minLat & latitude <= maxLat;
        k++;
      }
      return result;
    } else {
      return true;
    }

  }

  private static boolean stationFilter(String[] stations, String code) {

    if (stations != null) {
      int k = 0;
      boolean result = false;

      while (result == false & k < stations.length) {
        if (code.toLowerCase().compareTo(stations[k].toLowerCase()) == 0) {
          result = true;
        }
        k++;
      }
      return result;
    } else {
      return true;
    }
  }

  /**
   * Main method.
   * @param args String[]
   */
  public static void main(String[] args) throws FileNotFoundException, ParseException {

    int c;
    int i;
    int j;
    int rankID = 1;

    double[] circleFilter = null;
    double[] boxFilter = null;

    Stacov stacov;
    StringBuilder circle = new StringBuilder();
    StringBuilder box = new StringBuilder();
    String[] stations = null;

    boolean notAlreadyPrinted = true;
    boolean condition;

    if (args.length == 0) {
      System.out.println("Usage: StacovToSQL options stacovfile ...");
      System.out.println("");
      System.out.println("Reads stacov files and writes SQL statements to the standard output");
      System.out.println("suitable for insertion into a VALVE GPS database.  Multiple stacov");
      System.out.println("files may be given.");
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
      System.out.println("  --stations=<sta1>,<sta2>, ...");
      System.out.println("     Defines a stations filter. Stations on the list will pass");
      System.out.println("     through the filter.");
      System.out.println("");
      System.out.println("Notes:");
      System.out.println("");
      System.out.println("  Multiple geographic filters may be defined. Only one station filter");
      System.out.println("  may be defined.");
      System.out.println(" ");
      System.out.println("  Including a station name using the --stations argument does not    ");
      System.out.println("  guarantee the station will appear in the output.  It may be blocked");
      System.out.println("  by a geographic filter.");
      System.out.println("");
      System.out.println("  The 'circle' is actually a spherical cap, meaning that the");
      System.out.println("  radius value is a great circle distance.  Distance is computed on a");
      System.out.println("  sphere of radius = 6378137 meters. Likewise, the 'box' is");
      System.out.println("  really a latitude-longitude rectangle on a sphere.");
      System.out.println("");
      System.out.println("  The arguments can be given in any order. White space separates the");
      System.out.println("  argument.");
      System.out.println("");
      System.out.println("Version 0.9, June 30, 2011, report bugs to: pcervelli@usgs.gov");
      System.out.println("");
      return;
    }

    c = 0;
    while (args[c].startsWith("--")) {
      String key = args[c].split("=")[0].substring(2);
      String value = args[c].split("=")[1];
      if (key.toLowerCase().startsWith("c")) {
        circle.append(value).append(",");
      } else if (key.toLowerCase().startsWith("b")) {
        box.append(value).append(",");
      } else if (key.toLowerCase().startsWith("s")) {
        stations = value.split(",");
      } else if (key.toLowerCase().startsWith("r")) {
        rankID = Integer.parseInt(value);
      }
      c++;
    }

    if (circle.length() > 0) {
      String[] circles = circle.toString().split(",");
      circleFilter = new double[circles.length];
      for (i = 0; i < circles.length; i++) {
        circleFilter[i] = Double.parseDouble(circles[i]);
      }
    }

    if (box.length() > 0) {
      String[] boxes = box.toString().split(",");
      boxFilter = new double[boxes.length];
      for (i = 0; i < boxes.length; i++) {
        boxFilter[i] = Double.parseDouble(boxes[i]);
      }
    }

    for (i = c; i < args.length; i++) {

      stacov = new Stacov(args[i]);
      notAlreadyPrinted = true;

      if (stacov.isValid) {

        for (j = 0; j < stacov.nstations; j++) {

          double[] llh = stacov.llh(j);

          if (circleFilter == null | boxFilter == null) {
            condition =
                circleFilter(circleFilter, llh[0], llh[1]) & boxFilter(boxFilter, llh[0], llh[1])
                    & stationFilter(stations, stacov.code[j]);
          } else {
            condition = circleFilter(circleFilter, llh[0], llh[1])
                | boxFilter(boxFilter, llh[0], llh[1]) & stationFilter(stations, stacov.code[j]);
          }

          if (condition) {

            if (notAlreadyPrinted) {
              System.out.printf(
                  "INSERT IGNORE INTO sources (name,j2ksec0,j2ksec1,rid) VALUES "
                  + "(\"%s\",%f,%f,%s);\n", stacov.name, stacov.j2ksec0, stacov.j2ksec1, rankID);
              System.out.printf(
                  "SELECT sid FROM sources WHERE name=\"%s\" AND j2ksec0=%f AND j2ksec1=%f AND "
                  + "rid=%s INTO @sourceID;\n",
                  stacov.name, stacov.j2ksec0, stacov.j2ksec1, rankID);
              notAlreadyPrinted = false;
            }

            System.out.printf(
                "INSERT IGNORE INTO channels (code,lon,lat,height) VALUES "
                + "(\"%s\",%20.15f,%20.15f,%20.15f);\n", stacov.code[j], llh[0], llh[1], llh[2]);
            System.out.printf("SELECT cid FROM channels WHERE code=\"%s\" INTO @channelID;\n",
                stacov.code[j]);
            System.out.printf(
                "INSERT INTO solutions VALUES "
                + "(@sourceID,@channelID,%.17e,%.17e,%.17e,%.17e,%.17e,%.17e,%.17e,%.17e,%.17e);\n",
                stacov.data[0][j], stacov.data[1][j], stacov.data[2][j],
                stacov.covariance[0][j],
                stacov.covariance[1][j],
                stacov.covariance[2][j],
                stacov.covariance[3][j],
                stacov.covariance[4][j],
                stacov.covariance[5][j]);
          }

        }

      }

    }

  }

}
