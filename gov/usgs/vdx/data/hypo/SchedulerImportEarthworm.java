package gov.usgs.vdx.data.hypo;

import gov.usgs.util.Arguments;
import gov.usgs.util.ResourceReader;
import gov.usgs.util.Util;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;

/**
 * Class for importing earthworm format catalog files.
 *  
 * $Log: not supported by cvs2svn $
 * Revision 1.3  2007/11/13 22:02:49  uid894
 * Secondary Commit
 *
 * Revision 1.2  2007/11/13 21:32:37  uid894
 * Initial Commit
 *
 * Revision 1.1  2007/10/26 19:30:07  uid894
 * Initial Commit
 *
 * Revision 1.2  2007/07/24 23:10:27  tparker
 * add header
 *
 *
 * @author Loren Antolik
 */

public class SchedulerImportEarthworm extends SchedulerImporter 
{
	private SimpleDateFormat dateIn;
	private SimpleDateFormat dateOut;

/*
	public SchedulerImportEarthworm(SQLHypocenterDataSource ds)
	{
		super(ds);
		dateIn = new SimpleDateFormat("yyyyMMdd HHmm ss.SSS");
		dateIn.setTimeZone(TimeZone.getTimeZone("GMT"));
		dateOut = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		dateOut.setTimeZone(TimeZone.getTimeZone("GMT"));
	}
*/
	
	// all this constructor does is handle direct calls to this class from
	// the command line, thus through the main class
	public SchedulerImportEarthworm(String[] args) {
		init(args);
	}
	
	// this constructor gets called when we dynamically instantiate this
	// class from another class.  we can explicitly call the init class from 
	// the calling class
	public SchedulerImportEarthworm() {
	}
	
	// this function does the bulk of the work for instantiation.
	// it is programmed to be accessible either from running independently
	// or from instantiating this class from another class
	public void init(String[] args) {
		Arguments arguments			= new Arguments(args, flags, keys);	
		setDataSource(getDataSource(arguments));
		process(arguments, this);
	}
	
	public List<Hypocenter> importResource(String resource)
	{
		ResourceReader rr = ResourceReader.getResourceReader(resource);
		if (rr == null)
			return null;
		
		List<Hypocenter> hypos = new ArrayList<Hypocenter>();
		String s;
		int lines = 0;
		while ((s = rr.nextLine()) != null)
		{
			try
			{
				lines++;
				
				// DATE
				String ds		= s.substring(0, 19).trim() + "0";
				Date date		= dateIn.parse(ds);
				double j2ksec	= Util.dateToJ2K(date);

				// LAT
				double latdeg	= Double.parseDouble(s.substring(20, 22).trim());
				double latdec	= Double.parseDouble(s.substring(23, 28).trim());
				double lat		= latdeg + ( latdec / 60.0 );

				// LON
				double londeg	= Double.parseDouble(s.substring(29, 32).trim());
				double londec	= Double.parseDouble(s.substring(33, 38).trim());
				double lon		= londeg + ( londec / 60.0 );
				lon *= -1;
				
				// DEPTH
				double depth	= Double.parseDouble(s.substring(39, 45).trim());
				depth *= -1;
				
				// MAGNITUDE
				double mag		= Double.parseDouble(s.substring(47, 52).trim());

				System.out.println("EW: " + j2ksec + " " + dateOut.format(date) + " " + lon + " " + lat + " " + depth + " " + mag);
				Hypocenter ew	= new Hypocenter(new double[] {j2ksec, lon, lat, depth, mag});
				hypos.add(ew);
			}
			catch (Exception e)
			{
				System.err.println("Line " + lines + ": " + e.getMessage());
			}
		}
		rr.close();
		return hypos;
	}
	
	public static void main(String args[])	{
		
		// the only thing we are doing here is getting the command line arguments
		// we'll let the rest of the program do the majority of the work, because
		// it may be accessed from other classes as well that only have access to 
		// this class through a constructor with no arguments
		SchedulerImportEarthworm sie	= new SchedulerImportEarthworm(args);
	}	
}
