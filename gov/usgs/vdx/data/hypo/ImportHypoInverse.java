package gov.usgs.vdx.data.hypo;

import gov.usgs.util.Arguments;
import gov.usgs.util.ResourceReader;
import gov.usgs.util.Util;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;

public class ImportHypoInverse extends Importer 
{
	private SimpleDateFormat dateIn;
	
	public ImportHypoInverse(SQLHypocenterDataSource ds)
	{
		super(ds);
		dateIn = new SimpleDateFormat("yyyyMMddHHmmss.SS");
		dateIn.setTimeZone(TimeZone.getTimeZone("GMT"));
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
				String year		= s.substring(0,4);
				String monthDay	= s.substring(4,8);
				String hourMin	= s.substring(8,12);
				String sec		= s.substring(12,14);
				String secDec	= s.substring(14,16);
				
				Date date		= dateIn.parse(year+monthDay+hourMin+sec+"."+secDec);
				double j2ksec	= Util.dateToJ2K(date);

				// LAT
				double latdeg	= Double.parseDouble(s.substring(16, 18).trim());
				double latmin	= Double.parseDouble(s.substring(19, 21).trim() + "." + s.substring(21, 23).trim());
				double lat		= latdeg + ( latmin / 60.0d );
				char ns			= s.charAt(18);
				if (ns == 'S')
					lat *= -1;


				// LON
				double londeg	= Double.parseDouble(s.substring(23, 26).trim());
				double lonmin	= Double.parseDouble(s.substring(27, 29).trim() + "." + s.substring(29, 31).trim());
				double lon		= londeg + ( lonmin / 60.0d );
				char ew			= s.charAt(26);
				if (ew != 'E')
					lon *= -1;
				
				// DEPTH
				double depth	= Double.parseDouble(s.substring(31, 34).trim() + "." + s.substring(34, 36).trim());
				
				// MAGNITUDE
				double mag		= -99.99;
				try { mag = Double.parseDouble(s.substring(147, 150).trim()) / 100; } catch (Exception pe) {}
				
				if (!s.substring(45,46).equals(" "))
					throw new Exception("corrupt data at column 46");

				System.out.println("HC: " + j2ksec + " : " + lon + " : " + lat + " : " + depth + " : " + mag);
				Hypocenter hc	= new Hypocenter(new double[] {j2ksec, lon, lat, depth, mag});
				hypos.add(hc);
			}
			catch (Exception e)
			{
				System.err.println("Line " + lines + ": " + e.getMessage());
			}
		}
		rr.close();
		return hypos;
	}
	
	public static void main(String as[])
	{
		Arguments args = new Arguments(as, flags, keys);
		SQLHypocenterDataSource ds = Importer.getDataSource(args);
		process(args, new ImportHypoInverse(ds));
	}	
}
