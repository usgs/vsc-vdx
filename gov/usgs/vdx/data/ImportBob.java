package gov.usgs.vdx.data;

import gov.usgs.util.Arguments;
import gov.usgs.util.ConfigFile;
import gov.usgs.util.Time;
import gov.usgs.util.Util;
import gov.usgs.vdx.data.rsam.SQLEWRSAMDataSource;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import cern.colt.matrix.DoubleFactory2D;
import cern.colt.matrix.DoubleMatrix2D;

/**
 * 
 * $Log: not supported by cvs2svn $
 * Revision 1.4  2007/04/23 22:37:19  dcervelli
 * Flipped value boolean test.
 *
 * Revision 1.3  2007/04/23 22:36:22  dcervelli
 * Only keeps good samples.
 *
 * Revision 1.2  2007/04/23 22:14:58  dcervelli
 * Fixed dt computation.
 *
 * Revision 1.1  2007/04/23 21:54:04  dcervelli
 * Initial commit.
 *
 * @author Dan Cervelli
 * @version $Id: ImportBob.java,v 1.5 2007-04-30 05:27:32 tparker Exp $
 */
public class ImportBob
{
	public int goodCount;
	public double[] t;
	public float[] d;
	private int year;
	private static final String CONFIG_FILE = "vdx.config";
	public ConfigFile params;
	
	public ImportBob(String cf, int y, String n)
	{
		year = y;
		params = new ConfigFile(cf);
		params.put("vdx.databaseName", n);
		if (params == null)
			System.out.println("Can't parse config file " + cf);
	}
	
	public DoubleMatrix2D parseFile(String fn)
	{
		DoubleMatrix2D data = null;		
		
		try
		{
			DataInputStream dis = new DataInputStream(new BufferedInputStream(new FileInputStream(fn)));
			int absoluteRecordSize = Util.swap(dis.readShort());
			int samplesPerRecord = absoluteRecordSize / 4;
			
			double dt = 86400.0 / samplesPerRecord;
			double time = Time.parse("yyyyMMDD", year + "0101");
			dis.readShort(); // skip remaining 16-bits
			dis.skip(absoluteRecordSize - 4);
		    GregorianCalendar cal = new GregorianCalendar();
		    boolean leapYear = cal.isLeapYear(year);
		    int numRecords = 365;
		    if (leapYear)
		    	numRecords++;
		    
		    data = DoubleFactory2D.dense.make(numRecords * samplesPerRecord, 2);
		    System.err.println("records: " + numRecords);
		    System.err.println("record size: " + absoluteRecordSize);
		    System.err.println("expected samples: " + numRecords * samplesPerRecord);
		    System.err.println("expected filesize: " + absoluteRecordSize * (numRecords + 1));
		    goodCount = 0;
		    for (int i = 0; i < numRecords * samplesPerRecord; i++)
		    {
		    	float value = Float.intBitsToFloat(Util.swap(dis.readInt()));
		    	if (value != -998.0f)
		    	{
					data.setQuick(goodCount, 0, time);
					data.setQuick(goodCount++, 1, value);
//					System.out.println(t[i] + "," + d[i]);
		    	}
				time += dt;
			}
		    System.err.println("good count: " + goodCount);
		    dis.close();
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
		
		return(data);
	}
	
	public static void main(String[] as)
	{
		
		String cf = CONFIG_FILE;
		String name = "";
		String type = "";
		int year = 1970;
		String channel = "";
		
		Set<String> flags = new HashSet<String>();
		Set<String> kvs = new HashSet<String>();
		kvs.add("-c");
		kvs.add("-h");
		kvs.add("-s");
		kvs.add("-y");
		kvs.add("-n");
		kvs.add("-t");

		Arguments args = new Arguments(as, flags, kvs);
		
		if (args.contains("-c"))
			cf = args.get("-c");

		if (args.contains("-n"))
			name = args.get("-n");
		
		if (args.contains("-s"))
			channel = args.get("-s");

		if (args.contains("-y"))
			year = Integer.parseInt(args.get("-y"));
		
		
		if (args.contains("-t"))
			type = args.get("-t");
				
		if (args.contains("-h") || type == null)
		{
			System.err.println("java gov.usgs.vdx.data.gps.ImportNWIS [-c configFile] [-p period]");
			System.exit(-1);
		}
		
		List<String> files = args.unused();
		ImportBob in = new ImportBob(cf, year, name);
		
		Map<String, SQLDataSource> sources = new HashMap<String, SQLDataSource>();
		sources.put("ewrsam", new SQLEWRSAMDataSource());
		
		SQLDataSource sds = sources.get(type);
		if (sds == null)
		{
			System.out.println("I don't know what to do with source " + type);
			System.exit(-1);
		}
		
		sds.initialize(in.params);
		for (String f : files)
			sds.insertData(channel, in.parseFile(f));
	}
}
