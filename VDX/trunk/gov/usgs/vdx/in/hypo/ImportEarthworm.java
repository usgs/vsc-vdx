package gov.usgs.vdx.in.hypo;

import gov.usgs.util.Arguments;
import gov.usgs.util.ResourceReader;
import gov.usgs.util.Util;
import gov.usgs.vdx.data.hypo.Hypocenter;
import gov.usgs.vdx.data.hypo.SQLHypocenterDataSource;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;

/**
 * Class for importing earthworm format catalog files.
 *  
 * $Log: not supported by cvs2svn $
 *
 * @author Loren Antolik
 */

public class ImportEarthworm extends Importer 
{
	private SimpleDateFormat dateIn;
	private SimpleDateFormat dateOut;
	
	/**
	 * Constructor
	 * @param ds data source to import in
	 */
	public ImportEarthworm(SQLHypocenterDataSource ds)
	{
		super(ds);
		dateIn = new SimpleDateFormat("yyyyMMdd HHmm ss.SSS");
		dateIn.setTimeZone(TimeZone.getTimeZone("GMT"));
		dateOut = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		dateOut.setTimeZone(TimeZone.getTimeZone("GMT"));
	}
	
	/**
	 * Parse earthworm data from url (resource locator or file name)
	 * @param resource resource identifier
	 * @return Hypocenters list
	 */
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
				Hypocenter ew	= new Hypocenter(j2ksec, 0, lat, lon, depth, mag);
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
	
	/**
	 * Main method.
	 * Initialize data source using command line arguments and make import.
	 * Syntax is:
	 * "<importer> -c [vdx config] -n [database name] files..."
	 * @param as command line args
	 */
	public static void main(String as[])
	{
		Arguments args = new Arguments(as, flags, keys);
		SQLHypocenterDataSource ds = Importer.getDataSource(args);
		process(args, new ImportEarthworm(ds));
	}	
}
