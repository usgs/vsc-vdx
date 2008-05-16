package gov.usgs.vdx.data.wave;

import edu.iris.Fissures.seed.builder.SeedObjectBuilder;
import edu.iris.Fissures.seed.container.Blockette;
import edu.iris.Fissures.seed.container.Btime;
import edu.iris.Fissures.seed.container.SeedObjectContainer;
import edu.iris.Fissures.seed.container.Waveform;
import edu.iris.Fissures.seed.director.ImportDirector;
import edu.iris.Fissures.seed.director.SeedImportDirector;
import gov.usgs.util.Util;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

/**
 * A class for reading a Seed volume.  Uses Robert Casey's PDCC seed code.
 * 
 * $Log: not supported by cvs2svn $
 * Revision 1.2  2005/05/11 22:25:27  cervelli
 * Added support for location codes.
 *
 * Revision 1.1  2005/05/09 23:54:40  cervelli
 * Initial commit.
 *
 * @author Dan Cervelli
 */
public class Seed
{
	public static Map<String, List<Wave>> readSeed(String fn)
	{
		Map<String, List<Wave>> map = new HashMap<String, List<Wave>>();
		try
		{
			DataInputStream ls = new DataInputStream(new BufferedInputStream(
	                new FileInputStream(fn)));

			ImportDirector importDirector = new SeedImportDirector();
			SeedObjectBuilder objectBuilder = new SeedObjectBuilder();
			importDirector.assignBuilder(objectBuilder);  // register the builder with the director
			// begin reading the stream with the construct command
			importDirector.construct(ls);  // construct SEED objects silently
			SeedObjectContainer container = (SeedObjectContainer)importDirector.getBuilder().getContainer();
			
			Object object;
			container.iterate();
			while ((object = container.getNext()) != null)
			{
				Blockette b = (Blockette)object;
				if (b.getType() != 999)
					continue;
				String code = b.getFieldVal(4) + "$" + b.getFieldVal(6) + "$" + b.getFieldVal(7);
				String loc = (String)b.getFieldVal(5);
				if (loc.trim().length() > 0)
					code = code + "$" + loc;
				
				List<Wave> parts = map.get(code);
                if (parts == null)
                {
                	parts = new ArrayList<Wave>();
                	map.put(code, parts);
                }
                
                if (b.getWaveform() != null)
                {
	                Waveform wf = b.getWaveform();
                	Wave sw = new Wave();
	                sw.setSamplingRate(getSampleRate(((Integer)b.getFieldVal(10)).intValue(), ((Integer)b.getFieldVal(11)).intValue()));
	                Btime bTime = (Btime)b.getFieldVal(8);
	                sw.setStartTime(Util.dateToJ2K(btimeToDate(bTime)));
	                sw.buffer = wf.getDecodedIntegers();
	                sw.register();
	                parts.add(sw);
                }
			}
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
		return map;
	}
	 
	public static Date btimeToDate(Btime bt)
	{
    	Calendar cal = Calendar.getInstance(TimeZone.getTimeZone("GMT"));
    	cal.set(Calendar.YEAR, bt.getYear());
    	cal.set(Calendar.DAY_OF_YEAR, bt.getDayOfYear());
    	cal.set(Calendar.HOUR_OF_DAY, bt.getHour());
    	cal.set(Calendar.MINUTE, bt.getMinute());
    	cal.set(Calendar.SECOND, bt.getSecond());
    	cal.set(Calendar.MILLISECOND, bt.getTenthMill() / 10);
    	return cal.getTime();
	}
	
	public static float getSampleRate (double factor, double multiplier) {
        float sampleRate = (float) 10000.0;  // default (impossible) value;
        if ((factor * multiplier) != 0.0) {  // in the case of log records
            sampleRate = (float) (java.lang.Math.pow
                                      (java.lang.Math.abs(factor),
                                           (factor/java.lang.Math.abs(factor)))
                                      * java.lang.Math.pow
                                      (java.lang.Math.abs(multiplier),
                                           (multiplier/java.lang.Math.abs(multiplier))));
        }
        return sampleRate;
    }
}
