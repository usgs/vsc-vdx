package gov.usgs.vdx.data.gps;

import gov.usgs.util.CodeTimer;
import gov.usgs.util.Log;
import gov.usgs.util.ResourceReader;
import gov.usgs.util.Util;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.logging.Logger;

public class ImportStacov
{	
	private Logger logger;
	private Map<String, Benchmark> benchmarks;
	private SQLGPSDataSource dataSource;

	public ImportStacov()
	{
		dataSource = new SQLGPSDataSource();
		Map<String, Object> params = new HashMap<String, Object>();
		params.put("vdx.host", "localhost");
		params.put("vdx.name", "v");
		params.put("vdx.databaseName", "msh");
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
			CodeTimer ct = new CodeTimer("ImportStacov");
			String md5 = Util.md5Resource(fn);
			System.out.println("MD5: " + md5);
			ct.mark("compute md5");
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
			
			int sid = dataSource.insertSource(fn, md5, t0, t1, 7);
			ct.mark("prepare");
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
			ct.mark("read stations");
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
			ct.mark("read covariance");
			for (SolutionPoint sp : points)
			{
				Benchmark bm = benchmarks.get(sp.benchmark);
				if (bm == null)
				{
					int bid = dataSource.insertBenchmark(sp.benchmark, -999, -999, -99999, 1);
					bm = new Benchmark();
					bm.setCode(sp.benchmark);
					bm.setId(bid);
					benchmarks.put(sp.benchmark, bm);
					System.out.println("Created benchmark: " + sp.benchmark);
				}
				dataSource.insertSolution(sid, bm.getId(), sp.dp, 0);
//				System.out.println(sp.benchmark + " " + sp.dp);
			}
			ct.mark("write to database");
			ct.stop();
		}
		catch (Exception e)
		{
			e.printStackTrace();	
		}
	}

	public static void main(String args[])
	{
		ImportStacov is = new ImportStacov();
		for (int i = 0; i < args.length; i++)
			is.importFile(args[i]);
		/*
		ImportStacov is = new ImportStacov();
		is.importFile(args[0], false);
		System.exit(1);
		
		if (args.length == 0)
		{
			System.out.println("specify an input file or directory");	
			System.exit(1);
		}
		String d = null;
		String u = null;
		boolean ufd = false;
		if (args.length == 3)
		{
			d = args[1];
			u = args[2];	
		}
		else if (new File("ImportStacov.config").exists())
		{
			ConfigFile cf = new ConfigFile("ImportStacov.config");
			d = cf.getString("driver");
			u = cf.getString("url");
			if (cf.getString("useFileDate") != null && cf.getString("useFileDate").equals("true"))
				ufd = true;
		}
		else
		{
			System.out.println("you must specify a database driver and url either on the command line after the input file: [input file] [driver] [url]");
			System.out.println("or in a file called 'ImportStacov.config' with lines like:");
			System.out.println();
			System.out.println("driver=driver.class.name");
			System.out.println("url=jdbc://url");
			System.out.println("# useFileDate gets the date from the file name, not the stacov file.");
			System.out.println("# This is for processing more frequent than daily solutions.");
			System.out.println("useFileDate=false");
			
			System.exit(1);
		}
		
		initialize(d, u);
//		ImportStacov is = new ImportStacov();
		File f = new File(args[0]);
		if (f.isDirectory())
		{
			File files[] = f.listFiles();
			for (int i = 0; i < files.length; i++)
			{
				if (files[i].getName().endsWith(".stacov"))
					is.importFile(files[i].getPath(), ufd);
			}
		}
		else
			is.importFile(args[0], false);
			*/	
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
