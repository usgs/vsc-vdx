package gov.usgs.vdx.data.rsam;

import gov.usgs.util.Arguments;
import gov.usgs.util.ConfigFile;
import gov.usgs.vdx.data.ImportBob;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.Calendar;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


/**
 * 
 * $Log: not supported by cvs2svn $
 * Revision 1.3  2007/06/13 15:44:53  tparker
 * Add timer and -Y option
 *
 * Revision 1.2  2007/06/12 20:29:35  tparker
 * cleanup
 *
 * Revision 1.1  2007/06/12 17:09:58  tparker
 * initial commit
 *
 * @author Tom Parker
 * @version $Id: ImportEWRSAM.java,v 1.4 2007-07-02 22:58:59 tparker Exp $
 */
public class ImportEWRSAM 
{
	private static final String CONFIG_FILE = "importEWRSAM.config";
	private int year;
	public ConfigFile params;
	
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
		String channel;
		int fileYear;
		
		// look for standard bob filename
		Pattern p1 = Pattern.compile("(\\w{4})(\\d{4})\\.DAT$");
		Matcher m1 = p1.matcher(f.getName());
		
		// look for non-bob-compliant full SCN in filename. 
		// allow for non-standard UAF style EHZ4 component
		Pattern p2 = Pattern.compile("(\\w{4}):(\\w{3}4?):(\\w{2})(\\d{4})\\.DAT$");
		Matcher m2 = p2.matcher(f.getName());
		
		if (m1.matches())
		{
			channel = m1.group(1);
			if (channel.matches("\\w{3}_"))
				channel = channel.substring(0, 3);
			
			channel = params.getString(channel);
			fileYear = Integer.parseInt(m1.group(2));
		}
		else if (m2.matches())
		{
			channel = m2.group(1) + "$" + m2.group(2) + "$" + m2.group(3);
			fileYear = Integer.parseInt(m2.group(4));
		}
		else
		{
			System.out.println("ignoring poorly named file " + f);
			return;
		}
		
		if ((year != -1) && (fileYear != year))
			return;
		 
		System.out.println ("importing " + channel + " " + t + " data for the year " + fileYear + " from file " + f.getAbsolutePath());
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
		Calendar startTime = Calendar.getInstance();
		String cf = CONFIG_FILE;
		int y = -2;
		
		System.out.println("Starting import at " + startTime.toString());
		
		Set<String> flags = new HashSet<String>();
		Set<String> kvs = new HashSet<String>();
		kvs.add("-c");
		kvs.add("-y");
		flags.add("-a");
		flags.add("-Y");

		Arguments args = new Arguments(as, flags, kvs);
		
		if (args.contains("-c"))
			cf = args.get("-c");
		
		
		
		if (args.contains("-y")) 
			y = Integer.parseInt(args.get("-y"));
		else if (args.flagged("-Y"))
			y = startTime.get(Calendar.YEAR);
		else if (args.flagged("-a"))
			y = -1;
		
		if (args.contains("-h") || y == -2)
		{
			System.err.println("java gov.usgs.vdx.data.rsam.ImportEWRSAM [-c <configFile>] [-Y | -y <year> | -a]");
			System.err.println("\t-c <configFile>\tconfig to use, Default: importEWRSAM.config");
			System.err.println("\t-Y\timport data for this year");
			System.err.println("\t-y <year>\timport data for given year");
			System.err.println("\t-a\timport all data");
			System.exit(-1);
		}

		ImportEWRSAM in = new ImportEWRSAM(cf, y);
		in.process();
		
		Calendar endTime = Calendar.getInstance();
		double ellapsedTime = (endTime.getTimeInMillis() - startTime.getTimeInMillis()) / 1000;
		System.out.println("Import finished at " + endTime.toString());
		System.out.println("Elapsed time: " + ellapsedTime + " seconds");
	}
}