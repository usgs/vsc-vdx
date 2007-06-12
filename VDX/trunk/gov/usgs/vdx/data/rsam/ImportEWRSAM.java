package gov.usgs.vdx.data.rsam;

import gov.usgs.util.Arguments;
import gov.usgs.util.ConfigFile;
import gov.usgs.vdx.data.ImportBob;
import gov.usgs.vdx.data.SQLDataSource;
import gov.usgs.vdx.data.nwis.DataType;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


/**
 * 
 * $Log: not supported by cvs2svn $
 * @author Tom Parker
 * @version $Id: ImportEWRSAM.java,v 1.1 2007-06-12 17:09:58 tparker Exp $
 */
public class ImportEWRSAM 
{
	
	public int goodCount;
	public double[] t;
	public float[] d;
	private int year;
	private static final String CONFIG_FILE = "importEWRSAM.config";
	public ConfigFile params;
	Map<String, SQLDataSource> sources;
	private String vdxConfig;
	
	public ImportEWRSAM(String cf, int y)
	{
		params = new ConfigFile(cf);
		if (!params.wasSuccessfullyRead())
		{
			System.out.println(cf + ": could not read config file.");
			System.exit(1);
		}
		year = y;
		
	}
	
	public void process()
	{
		File valueDir = new File(params.getString("valueDir"));
		File eventDir = new File(params.getString("eventDir"));
    	
		try
    	{
    		validateDirectory(valueDir);
    		validateDirectory(eventDir);
    	}
    	catch (FileNotFoundException e)
    	{
    		System.out.println(e.getMessage());
    		e.printStackTrace();
    		System.exit(1);
    	}
    	
	    for (File f: valueDir.listFiles())
	    	process(f, "ewrsamValues");
	    
	    for (File f: eventDir.listFiles())
	    	process(f, "ewrsamEvents");
	}
	
	private void process(File f, String t)
	{
		Matcher m;
		
		Pattern p = Pattern.compile("(\\w{4})(\\d{4})\\.DAT$");
		m = p.matcher(f.getName());
		if (!m.matches())
		{
			System.out.println("ignoring poorly named file " + f);
			return;
		}
		
		String channel = m.group(1);
		if (channel.matches("\\w{3}_"))
			channel = channel.substring(0, 3);

		channel = params.getString(channel);
		int fileYear = Integer.parseInt(m.group(2));
		if ((year != -1) && (fileYear != year))
			return;
		
		System.out.println ("importing " + t + " data for " + channel + " from the year " + fileYear + " from file " + f.getAbsolutePath());
		ImportBob ib = new ImportBob(params.getString("vdxConfig"), fileYear, params.getString("vdxName"), t);	    		
		ib.process(channel, f.getAbsolutePath());
	}
	
	static private void validateDirectory (File aDirectory) throws FileNotFoundException 
	{
		if (aDirectory == null) 
		  throw new IllegalArgumentException("Directory should not be null.");
		else if (!aDirectory.exists())
		  throw new FileNotFoundException("Directory does not exist: " + aDirectory);
		else if (!aDirectory.isDirectory())
		  throw new IllegalArgumentException("Is not a directory: " + aDirectory);
		else if (!aDirectory.canRead())
		  throw new IllegalArgumentException("Directory cannot be read: " + aDirectory);
	}
 
	public static void main(String[] as)
	{
		String cf = CONFIG_FILE;
		int y = -2;
		
		Set<String> flags = new HashSet<String>();
		Set<String> kvs = new HashSet<String>();
		kvs.add("-c");
		kvs.add("-y");
		flags.add("-a");

		Arguments args = new Arguments(as, flags, kvs);
		
		if (args.contains("-c"))
			cf = args.get("-c");
		
		if (args.contains("-y"))
			y = Integer.parseInt(args.get("-y"));
		else if (args.flagged("-a"))
			y = -1;
		
		if (args.contains("-h") || y == -2)
		{
			System.err.println("java gov.usgs.vdx.data.rsam.ImportEWRSAM [-c <configFile>] [-y <year> | -a]");
			System.err.println("\t-c <configFile>\tconfig to use, Default: importEWRSAM.config");
			System.err.println("\t-y <year>\timport data for given year");
			System.err.println("\t-a\timport all data");
			System.exit(-1);
		}

		ImportEWRSAM in = new ImportEWRSAM(cf, y);
		in.process();
	}
}