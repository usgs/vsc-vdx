package gov.usgs.vdx.in.gps;

import gov.usgs.util.ConfigFile;
import gov.usgs.util.Log;
import gov.usgs.util.ResourceReader;
import gov.usgs.util.Util;
import gov.usgs.vdx.data.Channel;
import gov.usgs.vdx.data.gps.DataPoint;
import gov.usgs.vdx.data.gps.GPS;
import gov.usgs.vdx.data.gps.SQLGPSDataSource;

import java.io.File;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.logging.Logger;

public class SchedulerImportStacov implements gov.usgs.vdx.in.scheduler.Importer
{	
	private Logger logger;
	private Map<String, Channel> channels;
	private SQLGPSDataSource dataSource;
	private int typeId;
	
	/**
	 * Default constructor
	 */
	public SchedulerImportStacov() {
		
	}
	
	/**
	 * Constructor
	 * @param args command line arguments
	 */
	public SchedulerImportStacov(String[] args)	{
		init(args);
	}
	
	/**
	 * Initialize class object
	 * @param args command line arguments
	 */
	public void init (String[] args) {
		
		// check for the right number of args
		if (args.length < 3) {
			System.err.println("java gov.usgs.vdx.data.gps.SchedulerImportStacov [vdx prefix] [vdx name] [solution id] [files...]");
			System.exit(-1);
		}
		
		// parse the command line arguments
		typeId		= Integer.parseInt(args[2]);
		ConfigFile params = new ConfigFile();
		params.put("vdx.driver", "com.mysql.jdbc.Driver");
		params.put("vdx.url", "jdbc:mysql://localhost/?user=vdx&password=vdx");
		params.put("vdx.prefix", args[0]);
		params.put("vdx.name", args[1]);
		
		// instantiate the data source
		dataSource	= new SQLGPSDataSource();
		// TODO: work out new initialization
		// dataSource.initialize(params);
		
		// query and parse the benchmarks
		List<Channel> chs = dataSource.getChannelsList();
		channels	= new HashMap<String, Channel>();
		for (Channel ch : chs) {
			channels.put(ch.getCode(), ch);
		}

		// process the files from the command line
		for (int i = 3; i < args.length; i++) {
			importFile(args[i]);
		}
		
		// disconnect from the database
		dataSource.disconnect();
	}
	
	/**
	 * Import station covariance file
	 * @param fn file name
	 */
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
				
				sp.channel = sx.substring(7, 11).trim();
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
				
				Channel ch = channels.get(sp.channel);
				
				// if the channel isn't in the channel list from the db then it needs to be created
				if (ch == null) {
					double llh[] = GPS.xyz2LLH(sp.dp.x, sp.dp.y, sp.dp.z);
					dataSource.createChannel(sp.channel, sp.channel, llh[0], llh[1], llh[2]);
					ch = dataSource.getChannel(sp.channel);
					channels.put(sp.channel, ch);
					System.out.println("Created channel: " + sp.channel);
				}
				
				// insert the solution into the db
				dataSource.insertSolution(sid, ch.getId(), sp.dp);
			}
//			ct.mark("write to database");
//			ct.stop();
		}
		catch (Exception e)
		{
			e.printStackTrace();	
		}
	}

	/**
	 * Main method. Syntax:
	 * "java gov.usgs.vdx.data.gps.SchedulerImportStacov [vdx prefix] [vdx name] [solution id] [files...]"
	 */
	public static void main(String args[])
	{
		SchedulerImportStacov sis	= new SchedulerImportStacov(args);
	}	
	
	class SolutionPoint
	{
		public String channel;
		public DataPoint dp;
		
		public SolutionPoint()
		{
			dp = new DataPoint();
		}
	}
}
