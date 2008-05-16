package gov.usgs.vdx.data.gps;

import gov.usgs.util.ConfigFile;
import gov.usgs.util.Log;
import gov.usgs.util.ResourceReader;
import gov.usgs.util.Util;

import java.io.File;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.logging.Logger;

/**
 *
 * TODO: un-hardcode "localhost"
 * 
 * $Log: not supported by cvs2svn $
 * Revision 1.10  2006/04/04 15:50:49  dcervelli
 * Eliminated warnings.
 *
 * Revision 1.9  2006/03/02 20:45:34  cervelli
 * Adds LLH when creating new benchmark
 *
 * Revision 1.8  2005/10/21 21:22:23  tparker
 * Roll back changes related to Bug #77
 *
 * Revision 1.6  2005/10/13 20:38:11  dcervelli
 * Configurable stid, checks for already inserted hashes. sets filename properly.
 *
 * Revision 1.5  2005/09/05 18:43:42  dcervelli
 * Un-hardcoded database name.
 *
 * Revision 1.4  2005/09/05 18:37:16  dcervelli
 * Un-hardcoded database name.
 *
 * Revision 1.3  2005/09/05 00:41:32  dcervelli
 * Fixed covariance calculations.
 *
 * @author Dan Cervelli
 */
public class ImportStacov
{	
	private Logger logger;
	private Map<String, Benchmark> benchmarks;
	private SQLGPSDataSource dataSource;
	private int typeId;

	public ImportStacov(String vdxName, String dbName, int tid)
	{
		dataSource = new SQLGPSDataSource();
		typeId = tid;
		ConfigFile params = new ConfigFile();
		params.put("vdx.host", "localhost");
		params.put("vdx.name", vdxName);
		params.put("vdx.databaseName", dbName);
		dataSource.initialize(params);
		List<Benchmark> bms = dataSource.getBenchmarks();
		benchmarks = new HashMap<String, Benchmark>();
		for (Benchmark bm : bms)
			benchmarks.put(bm.getCode(), bm);
	}
	
	public void importFile(String fn)
	{
		logger = Log.getLogger("gov.usgs.vdx");
		try
		{
//			CodeTimer ct = new CodeTimer("ImportStacov");
			String md5 = Util.md5Resource(fn);
			System.out.println("MD5: " + md5);
//			ct.mark("compute md5");
			ResourceReader rr = ResourceReader.getResourceReader(fn);
			if (rr == null)
				return;
			logger.info("importing: " + fn);
			SimpleDateFormat dateIn;
			
			dateIn = new SimpleDateFormat("yyMMMdd");
			SimpleDateFormat dateOut = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
			dateIn.setTimeZone(TimeZone.getTimeZone("GMT"));
			dateOut.setTimeZone(TimeZone.getTimeZone("GMT"));
			
			String s = rr.nextLine();
			int numParams = Integer.parseInt(s.substring(0, 5).trim());
			SolutionPoint[] points = new SolutionPoint[numParams / 3];
			
			Date fileDate = dateIn.parse(s.substring(20, 27));
			fileDate.setTime(fileDate.getTime() + 12 * 60 * 60 * 1000);
			double t0 = (((double)fileDate.getTime() / (double)1000) - 946728000);
			double t1 = t0 + 86400;
			
			int sid = dataSource.insertSource(new File(fn).getName(), md5, t0, t1, typeId);
			if (sid == -1)
			{
				System.out.println("This file has already been imported.");
				return;
			}
//			ct.mark("prepare");
			for (int i = 0; i < numParams / 3; i++)
			{
				String sx = rr.nextLine();
				String sy = rr.nextLine();
				String sz = rr.nextLine();
				SolutionPoint sp = new SolutionPoint();
				
				sp.benchmark = sx.substring(7, 11).trim();
				sp.dp.t = (t0 + t1) / 2;
				sp.dp.x = Double.parseDouble(sx.substring(25, 47).trim());
				sp.dp.sxx = Double.parseDouble(sx.substring(53, 74).trim());
				
				sp.dp.y = Double.parseDouble(sy.substring(25, 47).trim());
				sp.dp.syy = Double.parseDouble(sy.substring(53, 74).trim());
				
				sp.dp.z = Double.parseDouble(sz.substring(25, 47).trim());
				sp.dp.szz = Double.parseDouble(sz.substring(53, 74).trim());
				
				points[i] = sp;
			}
//			ct.mark("read stations");
			boolean done = false;
			while (!done)
			{
				try
				{
					String sc = rr.nextLine();
					if (sc != null && sc.length() >= 2)
					{
						int p1 = Integer.parseInt(sc.substring(0, 5).trim()) - 1;
						int p2 = Integer.parseInt(sc.substring(5, 11).trim()) - 1;
						double data = Double.parseDouble(sc.substring(13).trim());
						if (p1 / 3 == p2 / 3)
						{
							SolutionPoint sp = points[p1 / 3];
							int i1 = Math.min(p1 % 3, p2 % 3);
							int i2 = Math.max(p1 % 3, p2 % 3);
							if (i1 == 0 && i2 == 1)
								sp.dp.sxy = data;
							else if (i1 == 0 && i2 == 2)
								sp.dp.sxz = data;
							else if (i1 == 1 && i2 == 2)
								sp.dp.syz = data;
						}
					}
					else
						done = true;
				}
				catch (NumberFormatException e)
				{
					done = true;	
				}
			}
			rr.close();
//			ct.mark("read covariance");
			for (SolutionPoint sp : points)
			{
				sp.dp.sxy = sp.dp.sxy * sp.dp.sxx * sp.dp.syy;
				sp.dp.sxz = sp.dp.sxz * sp.dp.sxx * sp.dp.szz;
				sp.dp.syz = sp.dp.syz * sp.dp.syy * sp.dp.szz;
				sp.dp.sxx = sp.dp.sxx * sp.dp.sxx;
				sp.dp.syy = sp.dp.syy * sp.dp.syy;
				sp.dp.szz = sp.dp.szz * sp.dp.szz;
				
				Benchmark bm = benchmarks.get(sp.benchmark);
				if (bm == null)
				{
					double llh[] = GPS.xyz2LLH(sp.dp.x, sp.dp.y, sp.dp.z);
					int bid = dataSource.insertBenchmark(sp.benchmark, llh[0], llh[1], llh[2], 1);
					bm = new Benchmark();
					bm.setCode(sp.benchmark);
					bm.setId(bid);
					benchmarks.put(sp.benchmark, bm);
					System.out.println("Created benchmark: " + sp.benchmark);
				}
				dataSource.insertSolution(sid, bm.getId(), sp.dp, 0);
			}
//			ct.mark("write to database");
//			ct.stop();
		}
		catch (Exception e)
		{
			e.printStackTrace();	
		}
	}

	public static void main(String args[])
	{
		if (args.length < 3)
		{
			System.err.println("java gov.usgs.vdx.data.gps.ImportStacov [vdx prefix] [vdx name] [solution id] [files...]");
			System.exit(-1);
		}
		ImportStacov is = new ImportStacov(args[0], args[1], Integer.parseInt(args[2]));
		for (int i = 3; i < args.length; i++)
			is.importFile(args[i]);
	}	
	
	class SolutionPoint
	{
		public String benchmark;
		public DataPoint dp;
		
		public SolutionPoint()
		{
			dp = new DataPoint();
		}
	}
}
