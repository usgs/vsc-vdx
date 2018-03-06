package gov.usgs.volcanoes.vdx.data.tilt;

import cern.colt.matrix.DoubleFactory2D;
import cern.colt.matrix.DoubleMatrix1D;
import cern.colt.matrix.DoubleMatrix1DProcedure;
import cern.colt.matrix.DoubleMatrix2D;
import cern.colt.matrix.linalg.Algebra;
import cern.jet.math.Functions;

import gov.usgs.volcanoes.core.configfile.ConfigFile;
import gov.usgs.volcanoes.core.legacy.Arguments;
import gov.usgs.volcanoes.core.math.DownsamplingType;
import gov.usgs.volcanoes.core.time.J2kSec;
import gov.usgs.volcanoes.vdx.data.SQLDataSourceDescriptor;
import gov.usgs.volcanoes.vdx.data.SQLDataSourceHandler;
import gov.usgs.volcanoes.vdx.server.BinaryResult;

import java.io.BufferedReader;
import java.io.FileReader;
import java.text.DecimalFormat;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TiltVelocity {

  private static final String DATASOURCE = "hvo_def_tilt";
  private static final Logger LOGGER = LoggerFactory.getLogger(TiltVelocity.class);

  private ConfigFile configFile;
  private String driver;
  private String url;
  private String prefix;
  private SQLTiltDataSource sqlDataSource;
  private SQLDataSourceHandler sqlDataSourceHandler;
  private SQLDataSourceDescriptor sqlDataSourceDescriptor;

  /**
   * Constructor.
   * @param file ConfigFile
   */
  public TiltVelocity(String file) {
    // Read config file
    configFile = new ConfigFile(file);

    // Configure vdx parameters
    driver = configFile.getString("vdx.driver");
    url = configFile.getString("vdx.url");
    prefix = configFile.getString("vdx.prefix");

    // Config sql datasource stuff
    sqlDataSourceHandler = new SQLDataSourceHandler(driver, url, prefix);
    sqlDataSourceDescriptor = sqlDataSourceHandler.getDataSourceDescriptor(DATASOURCE);
    sqlDataSource = (SQLTiltDataSource) sqlDataSourceDescriptor.getSQLDataSource();
  }

  public void deinitialize() {
    sqlDataSource.disconnect();
  }

  /**
   * Loop through all lines of a file, calling run() for each line.
   */
  public void runAllFromFile(String filename) {
    try {
      BufferedReader br = new BufferedReader(new FileReader(filename));
      String line;
      while ((line = br.readLine()) != null) {
        if (line.startsWith("#")) {
          System.out.println(line);
          continue;
        } else if (line.equals("")) {
          continue;
        }

        String[] params = line.split(",");

        String name = params[0];
        String channel = params[1];
        double start = Double.valueOf(params[2]);
        double end = Double.valueOf(params[3]);
        int samples = Integer.valueOf(params[4]);
        double threshold = Double.valueOf(params[5]);
        double highThreshold = Double.valueOf(params[6]);
        double residual = Double.valueOf(params[7]);

        run(channel, name, start, end, samples, threshold, highThreshold, residual);
        System.out.println("");
      }
      br.close();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  /**
   * Get check params from database then run check.
   */
  public void run(String name) {
    String channel = "";
    int mins = 0;
    int samples = 0;
    double threshold = 0.0;
    double highthreshold = 0.0;
    double residual = 0.0;
    double end = J2kSec.fromDate(new Date());
    double start = end - (mins * 60);

    run(channel, name, start, end, samples, threshold, highthreshold, residual);
  }

  /**
   * Run this velocity check.
   *
   * @param channel String
   * @param name String
   * @param mins int
   * @param samples int
   * @param threshold double
   * @param highthreshold double
   * @param residual double
   */
  public void run(String channel, String name, int mins, int samples, double threshold,
      double highthreshold, double residual) {
    double end = J2kSec.fromDate(new Date());
    double start = end - (mins * 60);

    run(channel, name, start, end, samples, threshold, highthreshold, residual);
  }

  private void run(String channel, String name, double start, double end, int samples,
      double threshold, double highThreshold, double residual) {
    DecimalFormat decimalOut = new DecimalFormat("#####.###");

    int cid = sqlDataSource.defaultGetChannel(channel, false).getCId();
    TiltData td = (TiltData) ((BinaryResult) sqlDataSource
        .getTiltData(cid, 0, start, end, 0, DownsamplingType.NONE, 0)).getData();
    DoubleMatrix2D data = DoubleFactory2D.dense.compose(new DoubleMatrix2D[][]{{
        td.getData().viewPart(0, 0, td.getData().rows(), 1),
        td.getData().viewPart(0, 2, td.getData().rows(), 2),
        td.getRotatedDataWithoutTime(0.0),
        td.getVelocityDataWithoutTime()
      }}
    );

    if (data.rows() >= samples) {
      double[] vals = getVelocity(data, samples);
      LOGGER.debug("Initial - V: {} ({}), R: {} ({})",
          decimalOut.format(vals[0]), threshold, decimalOut.format(vals[1]), residual);

      if (vals[0] >= threshold) {
        if (name.endsWith("Long")) {
          // for long alarms, we will check with fewer AND more SAMPLES
          // if either the low or the high are also above THRESHOLD, then take the largest
          double[] oldvals = vals;
          double[] lowvals = getVelocity(data, samples - 60);
          double[] hivals = getVelocity(data, samples + 60);
          if ((lowvals[0] > threshold) || (hivals[0] > threshold)) {
            vals = maxVelocity(vals, lowvals, hivals);
          }

          LOGGER.debug("Double checking the velocity.");
          LOGGER.debug("V: {}\tLow V: {}\tHigh V: {}\tMax V: {}",
              decimalOut.format(oldvals[0]), decimalOut.format(lowvals[0]),
              decimalOut.format(hivals[0]), decimalOut.format(vals[0]));
        } else {
          // for short alarms, double the SAMPLES to make sure that it is a real alarm
          vals = getVelocity(data, (samples * 2));

          LOGGER.debug("Double checking velocity using more samples.");
          LOGGER.debug("V: {} ({}), R: {} ({})\t(samples={})", decimalOut.format(vals[0]),
              threshold, decimalOut.format(vals[1]), residual, (samples * 2));
        }
      }

      if (vals[0] >= highThreshold) {
        if (vals[1] <= residual) {
          LOGGER.debug("Generating real alarm.");
          LOGGER.debug("Final: V: {} ({}), R: {} ({})", decimalOut.format(vals[0]),
              threshold, decimalOut.format(vals[1]), residual);
        } else {
          LOGGER.debug("Generating weak alarm.");
          LOGGER.debug("Final: V: {} ({}), R: {} ({})", decimalOut.format(vals[0]),
              threshold, decimalOut.format(vals[1]), residual);
        }
      }

      LOGGER.info("{},{}", decimalOut.format(vals[0]), decimalOut.format(vals[1]));
    } else {
      LOGGER.info("Not enough samples for a velocity test");
    }
  }

  private static double[] getVelocity(DoubleMatrix2D data, int samples) {
    // get all the data for an angle of 0
    // Filter out rows with nulls
    DoubleMatrix2D all = data.viewSelection(new DoubleMatrix1DProcedure() {
      public final boolean apply(DoubleMatrix1D m) {
        return !(Double.isNaN(m.get(5)));
      }
    }).copy();

    // only sample the last [SAMPLES] minutes
    if (all.rows() > samples) {
      all = all.viewPart(all.rows() - samples, 0, samples, all.columns());
    }

    // all ones
    DoubleMatrix2D ones = DoubleFactory2D.dense.make(all.rows(), 1, 1);
    // Time/ones
    DoubleMatrix2D gvals =
        DoubleFactory2D.dense
            .compose(new DoubleMatrix2D[][]{{all.viewPart(0, 0, all.rows(), 1), ones}
            });
    double t0 = gvals.getQuick(0, 0);
    for (int i = 0; i < gvals.rows(); i++) {
      gvals.setQuick(i, 0, gvals.getQuick(i, 0) - t0);
    }

    // magnitudes
    DoubleMatrix2D mvals = all.viewPart(0, 5, all.rows(), 1);
    DoubleMatrix2D m =
        Algebra.DEFAULT.mult(
            Algebra.DEFAULT.mult(Algebra.DEFAULT.inverse(Algebra
                .DEFAULT.mult(gvals.viewDice(), gvals)), gvals.viewDice()),
            mvals);
    DoubleMatrix2D r = Algebra.DEFAULT.mult(gvals, m);
    r = r.assign(mvals, Functions.minus);
    DoubleMatrix2D rvals = Algebra.DEFAULT.mult(r.viewDice(), r);
    return new double[]{Math.abs(m.getQuick(0, 0) * 3600), rvals.getQuick(0, 0)};
  }

  private static double[] maxVelocity(double[] vals, double[] lowvals, double[] hivals) {
    if ((hivals[0] >= lowvals[0]) && (hivals[0] >= vals[0])) {
      return hivals;
    }
    if ((lowvals[0] >= hivals[0]) && (lowvals[0] >= vals[0])) {
      return lowvals;
    }

    return vals;
  }

  /**
   * Expected Command-Line Args: -c: Channel -n: Name -m: Minutes -s: Samples -t: Threshold -h: High
   * Threshold -r: Residual -v: VDX Config -configFile: File containing many configs.
   *
   * @param as cli args
   */
  public static void main(String[] as) {
    Set<String> keys = new HashSet<String>();
    keys.add("-c");
    keys.add("-n");
    keys.add("-m");
    keys.add("-s");
    keys.add("-t");
    keys.add("-h");
    keys.add("-r");
    keys.add("-v");
    keys.add("-configFile");

    Arguments args = new Arguments(as, null, keys);
    TiltVelocity tv = new TiltVelocity(args.get("-v"));

    String file;

    if ((file = args.get("-configFile")) != null) {
      tv.runAllFromFile(file);
    } else {
      tv.run(args.get("c"), args.get("n"), Integer.valueOf(args.get("m")),
          Integer.valueOf(args.get("s")),
          Double.valueOf(args.get("t")), Double.valueOf(args.get("h")),
          Double.valueOf(args.get("r")));
    }

    tv.deinitialize();
  }

}
