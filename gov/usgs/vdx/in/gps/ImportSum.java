package gov.usgs.vdx.in.gps;

import gov.usgs.util.ConfigFile;
import gov.usgs.util.ResourceReader;
import gov.usgs.vdx.data.gps.SQLGPSDataSource;

/**
 * Import benchmarks from file
 * @author Dan Cervelli
 */
public class ImportSum
{
	private SQLGPSDataSource dataSource;

	/**
	 * Constructor
	 * @param prefix vdx prefix
	 * @param name vdx name
	 */
	public ImportSum(String prefix, String name)
	{
		dataSource = new SQLGPSDataSource();
		ConfigFile params = new ConfigFile();
		params.put("vdx.driver", "com.mysql.jdbc.Driver");
		params.put("vdx.url", "jdbc:mysql://localhost/?user=vdx&password=vdx");
		params.put("vdx.prefix", prefix);
		params.put("vdx.name", name);
		// TODO: work out new initialization
		// dataSource.initialize(params);
	}
	
	/**
	 * Import benchmarks file
	 * @param fn file name
	 */
	public void importSum(String fn) {
		
		ResourceReader rr;
		double lon, lat, dlon, dlat, height, sign;
		String bm;
		String [] ss;
		
		rr = ResourceReader.getResourceReader(fn);
		if (rr == null)
			return;
		
		ss		= rr.nextLine().split("\t");
		bm		= ss[1].toUpperCase();
		
		ss		= rr.nextLine().split("\t");		
		dlat	= Double.parseDouble(ss[1].trim());
		sign	= 1;
		if (dlat < 0)
			sign = -1;		
		dlat	= Math.abs(dlat);		
		lat		= dlat + Double.parseDouble(ss[2].trim()) / 60 + Double.parseDouble(ss[3].trim()) / 3600;
		lat	   *= sign;
		
		ss		= rr.nextLine().split("\t");
		dlon	= Double.parseDouble(ss[1].trim());
		sign	= 1;
		if (dlon < 0)
			sign = -1;		
		dlon	= Math.abs(dlon);
		lon		= dlon + Double.parseDouble(ss[2].trim()) / 60 + Double.parseDouble(ss[3].trim()) / 3600;
		lon	   *= sign;
		
		ss 		= rr.nextLine().split("\t");
		height	= Double.parseDouble(ss[1].trim());
		
		dataSource.createChannel(bm, bm, lon, lat, height);
		System.out.printf("%s %f %f %f\n", bm, lon, lat, height);
	}
	
	/**
	 * Main method
	 * @param args command line args
	 */
	public static void main(String[] args) {
		ImportSum is = new ImportSum(args[0], args[1]);
		for (int i = 2; i < args.length; i++)
			is.importSum(args[i]);
	}
}
