package gov.usgs.volcanoes.vdx.data.tilt;

import java.io.BufferedReader;
import java.io.FileReader;
import java.text.DecimalFormat;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;

import cern.colt.matrix.DoubleFactory2D;
import cern.colt.matrix.DoubleMatrix1D;
import cern.colt.matrix.DoubleMatrix1DProcedure;
import cern.colt.matrix.DoubleMatrix2D;
import cern.colt.matrix.linalg.Algebra;
import cern.jet.math.Functions;
import gov.usgs.math.DownsamplingType;
import gov.usgs.util.Arguments;
import gov.usgs.util.ConfigFile;
import gov.usgs.util.Util;
import gov.usgs.volcanoes.vdx.data.SQLDataSourceDescriptor;
import gov.usgs.volcanoes.vdx.data.SQLDataSourceHandler;
import gov.usgs.volcanoes.vdx.data.tilt.SQLTiltDataSource;
import gov.usgs.volcanoes.vdx.data.tilt.TiltData;
import gov.usgs.volcanoes.vdx.server.BinaryResult;

public class TiltVelocity {
    
    public static final String DATASOURCE = "hvo_def_tilt";

    private ConfigFile f;
    private String driver, url, prefix;
    private SQLTiltDataSource sqlDataSource;
    private SQLDataSourceHandler sqlDataSourceHandler;
    private SQLDataSourceDescriptor sqlDataSourceDescriptor;
    private boolean debug = false;

    public TiltVelocity(String file) {
        // Read config file
        f = new ConfigFile(file);

        // Configure vdx parameters
        driver = f.getString("vdx.driver");
        url    = f.getString("vdx.url");
        prefix = f.getString("vdx.prefix");

        // Config sql datasource stuff
        sqlDataSourceHandler    = new SQLDataSourceHandler(driver, url, prefix);
        sqlDataSourceDescriptor = sqlDataSourceHandler.getDataSourceDescriptor(DATASOURCE);
        sqlDataSource           = (SQLTiltDataSource)sqlDataSourceDescriptor.getSQLDataSource();
    }

    public void deinitialize() {
        sqlDataSource.disconnect();
    }
    
    /**
     * Loop through all lines of a file, calling run() for each line
     * @param filename
     */
    public void runAllFromFile(String filename) {
        debug = true;
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
                
                String name          = params[0];
                String channel       = params[1];
                double start         = Double.valueOf(params[2]);
                double end           = Double.valueOf(params[3]);
                int samples          = Integer.valueOf(params[4]);
                double threshold     = Double.valueOf(params[5]);
                double highThreshold = Double.valueOf(params[6]);
                double residual      = Double.valueOf(params[7]);
                
                run(channel, name, start, end, samples, threshold, highThreshold, residual);
                System.out.println("");
            }
            br.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    
    /**
     * Get check params from database then run check
     * @param name
     */
    public void run(String name) {
        String channel = "";
        int mins = 0;
        int samples = 0;
        double threshold = 0.0;
        double highthreshold = 0.0;
        double residual = 0.0;
        double end = Util.dateToJ2K(new Date());
        double start = end - (mins * 60);
        
        run(channel, name, start, end, samples, threshold, highthreshold, residual);
    }

    public void run(String channel, String name, int mins, int samples, double threshold, 
                    double highthreshold, double residual) {
        double end   = Util.dateToJ2K(new Date());
        double start = end - (mins * 60);
        
        run(channel, name, start, end, samples, threshold, highthreshold, residual);
    }
    
    private void run(String channel, String name, double start, double end, int samples, 
                    double threshold, double highThreshold, double residual) {
        DecimalFormat decimalOut = new DecimalFormat("#####.###");
        
        int cid             = sqlDataSource.defaultGetChannel(channel, false).getCID();
        TiltData td         = (TiltData)((BinaryResult)sqlDataSource.getTiltData(cid, 0, start, end, 0, DownsamplingType.NONE, 0)).getData();
        DoubleMatrix2D data = DoubleFactory2D.dense.compose(new DoubleMatrix2D[][] {{
            td.getData().viewPart(0, 0, td.getData().rows(), 1),
            td.getData().viewPart(0, 2, td.getData().rows(), 2),
            td.getRotatedDataWithoutTime(0.0),
            td.getVelocityDataWithoutTime()
        }});
        
        if (data.rows() >= samples) {
            double[] vals = getVelocity(data, samples);
            if (debug) {
                System.out.println("Initial - V: " + decimalOut.format(vals[0]) + " (" + threshold + "), R: " + 
                                    decimalOut.format(vals[1]) + " (" + residual + ")");
            }
            
            if (vals[0] >= threshold) {
                if (name.endsWith("Long")) {
                    // for long alarms, we will check with fewer AND more SAMPLES
                    // if either the low or the high are also above THRESHOLD, then take the largest
                    double[] oldvals = vals;
                    double[] lowvals = getVelocity(data, samples - 60);
                    double[] hivals  = getVelocity(data, samples + 60);
                    if ((lowvals[0] > threshold) || (hivals[0] > threshold)) {
                        vals = maxVelocity(vals, lowvals, hivals);  
                    }
                    
                    if (debug) {
                        System.out.println("Double checking the velocity.");
                        System.out.println("V: " + decimalOut.format(oldvals[0]) + 
                                           "\tLow V: " + decimalOut.format(lowvals[0]) + 
                                           "\tHigh V: " + decimalOut.format(hivals[0]) + 
                                           "\tMax V: " + decimalOut.format(vals[0]));
                    }
                } else {
                    // for short alarms, double the SAMPLES to make sure that it is a real alarm
                    vals = getVelocity(data, (samples * 2));
                    
                    if (debug) {
                        System.out.println("Double checking velocity using more samples.");
                        System.out.println("V: " + decimalOut.format(vals[0]) + " (" + threshold + "), R: " + 
                                            decimalOut.format(vals[1]) + "(" + residual + ")\t(samples=" + (samples * 2) + ")");
                    }
                }
            }
            
            if (vals[0] >= highThreshold) {
                if (vals[1] <= residual) {
                    if (debug) {
                        System.out.println("Generating real alarm.");
                        System.out.println("Final: V: " + decimalOut.format(vals[0]) + " (" + threshold + "), R: " + 
                                            decimalOut.format(vals[1]) + "(" + residual + ")");
                    }
                } else {
                    if (debug) {
                        System.out.println("Generating weak alarm.");
                        System.out.println("Final: V: " + decimalOut.format(vals[0]) + " (" + threshold + "), R: " + 
                                            decimalOut.format(vals[1]) + "(" + residual + ")");
                    }
                }
            }
            
            System.out.println(decimalOut.format(vals[0]) + "," + decimalOut.format(vals[1]));
        } else {
            System.out.println("Not enough samples for a velocity test");
        }
    }
    
    private static double[] getVelocity(DoubleMatrix2D data, int samples) {
        // get all the data for an angle of 0       
        // Filter out rows with nulls
        DoubleMatrix2D all = data.viewSelection(new DoubleMatrix1DProcedure() {
           public final boolean apply(DoubleMatrix1D m) { return !(Double.isNaN(m.get(5))); } 
        }).copy();
        
        // only sample the last [SAMPLES] minutes
        if (all.rows() > samples)
            all = all.viewPart(all.rows() - samples, 0, samples, all.columns());

        // all ones
        DoubleMatrix2D ones = DoubleFactory2D.dense.make(all.rows(), 1, 1);
        // Time/ones
        DoubleMatrix2D G =
            DoubleFactory2D.dense.compose(new DoubleMatrix2D[][] { { all.viewPart(0, 0, all.rows(), 1), ones }
        });
        double t0 = G.getQuick(0, 0);
        for (int i = 0; i < G.rows(); i++)
            G.setQuick(i, 0, G.getQuick(i, 0) - t0);
            
        // magnitudes
        DoubleMatrix2D M = all.viewPart(0, 5, all.rows(), 1);
        DoubleMatrix2D m =
            Algebra.DEFAULT.mult(
                Algebra.DEFAULT.mult(Algebra.DEFAULT.inverse(Algebra.DEFAULT.mult(G.viewDice(), G)), G.viewDice()),
                M);
        DoubleMatrix2D r = Algebra.DEFAULT.mult(G, m);
        r = r.assign(M, Functions.minus);
        DoubleMatrix2D R = Algebra.DEFAULT.mult(r.viewDice(), r);
        return new double[] { Math.abs(m.getQuick(0, 0) * 3600), R.getQuick(0, 0)};   
    }
    
    private static double[] maxVelocity(double[] vals, double[] lowvals, double[] hivals) {
        if ((hivals[0] >= lowvals[0]) && (hivals[0] >= vals[0]))
            return hivals;
        if ((lowvals[0] >= hivals[0]) && (lowvals[0] >= vals[0]))
            return lowvals;
        
        return vals;
    }

    /**
     * Expected Command-Line Args:
     * -c: Channel
     * -n: Name
     * -m: Minutes
     * -s: Samples
     * -t: Threshold
     * -h: High Threshold
     * -r: Residual
     * -v: VDX Config
     * -f: File containing many configs
     * 
     * @param as: cli args
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
        keys.add("-f");

        Arguments args = new Arguments(as, null, keys);
        TiltVelocity tv  = new TiltVelocity(args.get("-v"));
        
        String file;
        
        if ((file = args.get("-f")) != null) {
            tv.runAllFromFile(file);
        } else {
            tv.run(args.get("c"), args.get("n"), Integer.valueOf(args.get("m")), Integer.valueOf(args.get("s")), 
                   Double.valueOf(args.get("t")), Double.valueOf(args.get("h")), Double.valueOf(args.get("r")));
        }
        
        tv.deinitialize();
    }

}
