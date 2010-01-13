package gov.usgs.vdx.in.gps;

import gov.usgs.util.ConfigFile;
import gov.usgs.util.ResourceReader;
import gov.usgs.util.Util;
import gov.usgs.vdx.data.Channel;
import gov.usgs.vdx.data.gps.SolutionPoint;
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

/**
 * Import station covariance file
 * 
 * @author Dan Cervelli, Loren Antolik
 */
public class ImportStacov {	
	private SQLGPSDataSource dataSource;
	private Map<String, Channel> channels;
	private int rid;
	private Logger logger;

	/**
	 * Constructor
	 * @param dataSource	name of the data source in vdxSources.config
	 * @param rank			rank from the ranks table
	 */
	public ImportStacov(String ds, int rank) {
		logger = Logger.getLogger("gov.usgs.vdx");
		
		// open VDX.config and get the driver, url and prefix
		ConfigFile params	= new ConfigFile("VDX.config");
		params.put("dbname", "hvo_deformation");
		
		// open vdxSources.config and get the database name
		String name = ds;
		
		// initialize the data source
		dataSource = new SQLGPSDataSource();
		dataSource.initialize(params);
		logger.info("Initialized data source for " + name);
		
		// define the rid from the user defined rank
		rid	= dataSource.defaultGetRankID(rank);
		if (rid == -1) {
			logger.severe("Rank not found in database");
			System.exit(-1);
		}
		
		// get the list of channels and create a hash map keyed with the channel code
		List<Channel> chs = dataSource.getChannelsList();
		channels = new HashMap<String, Channel>();
		for (Channel ch : chs) {
			channels.put(ch.getCode(), ch);
		}
	}
	
	/**
	 * Import station covariance file
	 * @param fn file name
	 */
	public void importFile(String filename) {
		
		String md5, s, sx, sy, sz, sc;
		int numParams, sid, p1, p2, i1, i2;
		double t0, t1, data;
		double llh[];
		Date fileDate;
		SimpleDateFormat dateIn, dateOut;
		SolutionPoint sp;
		SolutionPoint[] points;
		ResourceReader rr;
		Channel ch;
		boolean done;
		
		try {			
			dateIn	= new SimpleDateFormat("yyMMMdd");
			dateOut	= new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
			dateIn.setTimeZone(TimeZone.getTimeZone("GMT"));
			dateOut.setTimeZone(TimeZone.getTimeZone("GMT"));
			
			md5	= Util.md5Resource(filename);
			
			// check that the file exists
			rr	= ResourceReader.getResourceReader(filename);
			if (rr == null) {
				logger.severe("skipping: " + filename + " (resource is invalid)");
				return;
			}
			
			// check that the file has data
			s	= rr.nextLine();
			if (s == null)	{
				logger.severe("skipping: " + filename + " (resource is empty)");
				return;
			}
			
			numParams	= Integer.parseInt(s.substring(0, 5).trim());
			points		= new SolutionPoint[numParams / 3];
			
			fileDate	= dateIn.parse(s.substring(20, 27));
			fileDate.setTime(fileDate.getTime() + 12 * 60 * 60 * 1000);
			
			t0			= (((double)fileDate.getTime() / (double)1000) - 946728000);
			t1			= t0 + 86400;
			
			// attempt to insert this source.  this method will tell if this file has already been imported
			sid	= dataSource.insertSource(new File(filename).getName(), md5, t0, t1, rid);
			if (sid == -1) {
				logger.severe("skipping: " + filename + " (hash already exists)");
				return;
			} else {
				logger.info("importing: " + filename);
			}
			
			for (int i = 0; i < numParams / 3; i++) {
				sx	= rr.nextLine();
				sy	= rr.nextLine();
				sz	= rr.nextLine();
				sp	= new SolutionPoint();
				
				sp.channel	= sx.substring(7, 11).trim();
				sp.dp.t		= (t0 + t1) / 2;
				sp.dp.x		= Double.parseDouble(sx.substring(25, 47).trim());
				sp.dp.sxx	= Double.parseDouble(sx.substring(53, 74).trim());
				
				sp.dp.y		= Double.parseDouble(sy.substring(25, 47).trim());
				sp.dp.syy	= Double.parseDouble(sy.substring(53, 74).trim());
				
				sp.dp.z		= Double.parseDouble(sz.substring(25, 47).trim());
				sp.dp.szz	= Double.parseDouble(sz.substring(53, 74).trim());
				
				points[i]	= sp;
			}
			
			done = false;
			while (!done) {
				try {
					sc = rr.nextLine();
					if (sc != null && sc.length() >= 2) {
						p1		= Integer.parseInt(sc.substring(0, 5).trim()) - 1;
						p2		= Integer.parseInt(sc.substring(5, 11).trim()) - 1;
						data	= Double.parseDouble(sc.substring(13).trim());
						if (p1 / 3 == p2 / 3) {
							sp	= points[p1 / 3];
							i1	= Math.min(p1 % 3, p2 % 3);
							i2	= Math.max(p1 % 3, p2 % 3);
							if (i1 == 0 && i2 == 1)
								sp.dp.sxy = data;
							else if (i1 == 0 && i2 == 2)
								sp.dp.sxz = data;
							else if (i1 == 1 && i2 == 2)
								sp.dp.syz = data;
						}
					} else {
						done = true;
					}
				} catch (NumberFormatException e) {
					done = true;	
				}
			}
			rr.close();
			for (SolutionPoint spt : points) {
				spt.dp.sxy = spt.dp.sxy * spt.dp.sxx * spt.dp.syy;
				spt.dp.sxz = spt.dp.sxz * spt.dp.sxx * spt.dp.szz;
				spt.dp.syz = spt.dp.syz * spt.dp.syy * spt.dp.szz;
				spt.dp.sxx = spt.dp.sxx * spt.dp.sxx;
				spt.dp.syy = spt.dp.syy * spt.dp.syy;
				spt.dp.szz = spt.dp.szz * spt.dp.szz;
				
				ch	= channels.get(spt.channel);
				
				// if the channel isn't in the channel list from the db then it needs to be created
				if (ch == null) {
					llh	= GPS.xyz2LLH(spt.dp.x, spt.dp.y, spt.dp.z);
					dataSource.createChannel(spt.channel, spt.channel, llh[0], llh[1], llh[2]);
					ch	= dataSource.getChannel(spt.channel);
					channels.put(spt.channel, ch);
				}
				
				// insert the solution into the db
				dataSource.insertSolution(sid, ch.getId(), spt.dp);
			}

		} catch (Exception e) {
			e.printStackTrace();	
		}
	}

	/**
	 * Main method
	 */
	public static void main(String args[]) {
		
		int rank = -1;
		
		// check that the mininum number of parameters were passed
		if (args.length < 3) {
			System.err.println("java gov.usgs.vdx.data.in.gps.ImportStacov [data source] [rank] [files...]");
			System.exit(-1);
		}
		
		// check that the rank is actually an integer
		try {
			rank = Integer.parseInt(args[1]);
		} catch (NumberFormatException e) {
			System.err.println("specified rank is not a number");
			System.exit(-1);
		}
		
		ImportStacov is = new ImportStacov(args[0], rank);
		
		for (int i = 2; i < args.length; i++) {
			is.importFile(args[i]);
		}
	}
}
