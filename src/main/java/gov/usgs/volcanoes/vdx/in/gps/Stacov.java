package gov.usgs.volcanoes.vdx.in.gps;

import gov.usgs.volcanoes.core.time.J2kSec;
import gov.usgs.volcanoes.vdx.data.gps.Gps;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Scanner;
import java.util.TimeZone;

public class Stacov {

  public String fileName;
  public String name;
  public String[] code;

  public int nstations;
  public double j2ksec0;
  public double j2ksec1;
  public double[][] data;
  public double[][] covariance;

  public boolean isValid = true;

  private static double parseTime(String timeStamp) throws ParseException {

    String pattern = "yyMMMdd";
    SimpleDateFormat format = new SimpleDateFormat(pattern);
    format.setTimeZone(TimeZone.getTimeZone("UTC"));
    double t = J2kSec.fromDate(format.parse(timeStamp));
    return t;

  }

  /**
   * llh.
   * @param i int
   */
  public double[] llh(int i) {
    if (i < nstations) {
      return Gps.xyz2llh(data[0][i], data[1][i], data[2][i]);
    } else {
      return null;
    }
  }

  public Stacov(String stacovFile) throws FileNotFoundException, ParseException {
    fileName = stacovFile;
    this.read();
  }

  private void read() throws FileNotFoundException, ParseException {
    Scanner s;
    int j;

    s = new Scanner(new BufferedReader(new FileReader(fileName)));
    if (!s.hasNextInt()) {
      isValid = false;
      return;
    }
    nstations = s.nextInt() / 3;

    if (!s.next().equals("PARAMETERS")) {
      isValid = false;
      return;
    }

    name = fileName.split("/")[fileName.split("/").length - 1];

    s.next();
    j2ksec0 = Stacov.parseTime(s.next());
    j2ksec1 = j2ksec0 + 86400;

    data = new double[3][nstations];
    covariance = new double[6][nstations];
    code = new String[nstations];

    int c;
    int i;
    for (i = 0; i < nstations; i++) {
      for (j = 0; j < 3; j++) {
        s.next();
        code[i] = s.next();
        s.next();
        s.next();
        data[j][i] = s.nextDouble();
        s.next();
        covariance[j][i] = s.nextDouble();
      }
    }

    c = 3;
    while (s.hasNextInt()) {
      i = (s.nextInt() - 1) / 3;
      j = (s.nextInt() - 1) / 3;
      if (i == j) {
        covariance[c][i] = s.nextDouble();
        c++;
        if (c == 6) {
          c = 3;
        }
      } else {
        s.nextDouble();
      }
    }

    s.close();

    for (i = 0; i < nstations; i++) {
      covariance[3][i] = covariance[0][i] * covariance[1][i] * covariance[3][i];
      covariance[4][i] = covariance[0][i] * covariance[2][i] * covariance[4][i];
      covariance[5][i] = covariance[1][i] * covariance[2][i] * covariance[5][i];
      covariance[0][i] = covariance[0][i] * covariance[0][i];
      covariance[1][i] = covariance[1][i] * covariance[1][i];
      covariance[2][i] = covariance[2][i] * covariance[2][i];
    }

  }
}
