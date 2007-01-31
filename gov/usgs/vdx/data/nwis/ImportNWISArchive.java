package gov.usgs.vdx.data.nwis;

import gov.usgs.util.Arguments;
import gov.usgs.util.ConfigFile;
import gov.usgs.util.Log;
import gov.usgs.util.ResourceReader;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.TimeZone;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 *
  * $Log: not supported by cvs2svn $
  *
 *
 * @author Tom Parker
 */
public class ImportNWISArchive
{	
	private static final String CONFIG_FILE = "NWIS.config";
	private Logger logger;
	private Station st;
	private DataType dt;
	private SQLNWISDataSource dataSource;
	private ConfigFile params;

	public ImportNWISArchive(String cf)
	{
		dataSource = new SQLNWISDataSource();
		params = new ConfigFile(cf);
		
		dataSource.initialize(params);
		//dataSource.setName(params.getString("vdx.name"));
		//stations = dataSource.getStations();
	}
	
	public void importFile(String fn)
	{
		List<DataType> dataTypes = new ArrayList<DataType>();
		logger = Log.getLogger("gov.usgs.vdx");
		
		try
		{
			ResourceReader rr = ResourceReader.getResourceReader(fn);
			if (rr == null)
				return;
			logger.info("importing: " + fn);
			
			String s;
			boolean next = false;
			Pattern p;
			Matcher m;
			
			//read header
			s = rr.nextLine();	
			Pattern commentP = Pattern.compile("^#.*$");
			//Pattern stationPattern = Pattern.compile("//STATION AGENCY=\"(\\w*)\" NUMBER=\"(\\d*)");
			Pattern stationPattern = Pattern.compile("^.*//STATION AGENCY=\"(\\w*)\\s*\" NUMBER=\"(\\d*).*$");
			Pattern codePattern = Pattern.compile("^.*//PARAMETER CODE=\"(\\d*)\\s*\".*$");
			Pattern namePattern = Pattern.compile("^.*//PARAMETER LNAME=\"([^\"]*)\".*$");
			int code = 0;
			String name = null;
			Matcher commentM = commentP.matcher(s);
			while (commentM.matches())
			{
//				System.out.println("head matched " + s);
				Matcher stationMatcher = stationPattern.matcher(s);
				if (stationMatcher.matches())
				{
					String agency = stationMatcher.group(1).trim();
					String number = stationMatcher.group(2).trim();
					
					st = dataSource.getStation(agency, number);
					
//					System.out.println("Found station " + agency + number + " :: " + st.getName());
					
				}
				
				Matcher codeMatcher = codePattern.matcher(s);
				if (codeMatcher.matches())
					code = Integer.parseInt(codeMatcher.group(1).trim());
				
				Matcher nameMatcher = namePattern.matcher(s);
				if (nameMatcher.matches())
					name = nameMatcher.group(1).trim();

				if (dt == null && code > 0 && name != null)
					dt = new DataType(code, name);
				
				s = rr.nextLine();	
				commentM = commentP.matcher(s);
			}
			
			System.out.println("Found data type " + dt.getName());
			//drop 2 rows of column labels
			s = rr.nextLine();
			s = rr.nextLine();
			
			SimpleDateFormat dateIn = new SimpleDateFormat("yyyyMMdd HHmmss");
			dateIn.setTimeZone(TimeZone.getTimeZone(st.getTz()));
			Pattern dataPattern = Pattern.compile("^(\\d{8})\\s+(\\d{6})\\s+\\w{3}\\s+(\\d+.\\d+)\\s+.*$");
			Matcher dataMatcher = dataPattern.matcher(s);
			while (dataMatcher.matches())
			{
				System.out.println("Found data line " + s);
				Date date = dateIn.parse(dataMatcher.group(1) + " " + dataMatcher.group(2));
				dataSource.insertRecord(date, st, dt, Double.parseDouble(dataMatcher.group(3)), true);
				System.out.println("Found data " + dataMatcher.group(3));
				
				s = rr.nextLine();
				if (s != null)
					dataMatcher = dataPattern.matcher(s);
			}
		}
		catch (Exception e)
		{
			e.printStackTrace();	
		}
	}
	

	public static void main(String[] as)
	{
		String cf = CONFIG_FILE;
		int period = 1;
		Set<String> flags;
		Set<String> keys;

		flags = new HashSet<String>();
		keys = new HashSet<String>();
		keys.add("-c");
		keys.add("-h");

		Arguments args = new Arguments(as, flags, keys);
		
		if (args.contains("-h"))
		{
			System.err.println("java gov.usgs.vdx.data.nwis.ImportNWISArchive [-c configFile] file ...");
			System.exit(-1);
		}
		
		if (args.contains("-c"))
			cf = args.get("-c");

		ImportNWISArchive in = new ImportNWISArchive(cf);
		for (String file : args.unused())
			in.importFile(file);
	}	
}
