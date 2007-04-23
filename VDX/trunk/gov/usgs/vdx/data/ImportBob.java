package gov.usgs.vdx.data;

import gov.usgs.util.Time;
import gov.usgs.util.Util;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.util.GregorianCalendar;

/**
 * 
 * $Log: not supported by cvs2svn $
 * Revision 1.2  2007/04/23 22:14:58  dcervelli
 * Fixed dt computation.
 *
 * Revision 1.1  2007/04/23 21:54:04  dcervelli
 * Initial commit.
 *
 * @author Dan Cervelli
 * @version $Id: ImportBob.java,v 1.3 2007-04-23 22:36:22 dcervelli Exp $
 */
public class ImportBob
{
	public int goodCount;
	public double[] t;
	public float[] d;
	
	public ImportBob(String fn, int year)
	{
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
		    
		    t = new double[numRecords * samplesPerRecord];
		    d = new float[numRecords * samplesPerRecord];
		    System.err.println("records: " + numRecords);
		    System.err.println("record size: " + absoluteRecordSize);
		    System.err.println("expected samples: " + numRecords * samplesPerRecord);
		    System.err.println("expected filesize: " + absoluteRecordSize * (numRecords + 1));
		    goodCount = 0;
		    for (int i = 0; i < numRecords * samplesPerRecord; i++)
		    {
		    	float value = Float.intBitsToFloat(Util.swap(dis.readInt()));
		    	if (value == -998.0f)
		    	{
					t[goodCount] = time;
					d[goodCount] = value;
					goodCount++;
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
	}
	
	public static void main(String[] args)
	{
		new ImportBob(args[0], Integer.parseInt(args[1]));
	}
}
