package gov.usgs.vdx.data.tilt.ptx;

import gov.usgs.util.Util;

import java.text.SimpleDateFormat;
import java.util.TimeZone;

/**
 * 
 * $Log: not supported by cvs2svn $
 * @author Dan Cervelli
 */
public class PTXRecord
{
	public static final double VOLTS_PER_COUNT = 2.980232e-7;
	public static final int MAX_BIT = 16777215;
	
	private byte[] buffer;
	
	public PTXRecord(byte[] b)
	{
		buffer = b;
	}
	
	private long createInt(byte b0, byte b1, byte b2, byte b3)
	{
		return ((b0 & 0xff) | 
			   (((b1 & 0xff) << 8) & 0x0000ff00) |
			   (((b2 & 0xff) << 16) & 0x00ff0000)  |
			   (((b3 & 0xff) << 24) & 0xff000000) & 0x00000000ffffffff); 
	}
	
	public int getNumBytes()
	{
		return (int)createInt(buffer[0], buffer[1], (byte)0, (byte)0);
	}
	
	public long getStartTime()
	{
		return createInt(buffer[5], buffer[4], buffer[3], buffer[2]);
	}
	
	public int getTemperature()
	{
		return (int)createInt(buffer[6], buffer[7], buffer[8], (byte)0);	
	}
	
	public int getVoltage()
	{
		return (int)createInt(buffer[9], buffer[10], buffer[11], (byte)0);	
	}
	
	public int getHeading()
	{
		return (int)createInt(buffer[12], buffer[13], (byte)0, (byte)0);
	}
	
	public int getSecondsPerSample()
	{
		return (int)createInt(buffer[14], (byte)0, (byte)0, (byte)0); 
	}

	public int getGain()
	{
		return buffer[15] + 1;
	}
	
	public int getSerialNumber()
	{
		return (int)createInt(buffer[16], buffer[17], (byte)0, (byte)0);
	}
	
	public int getVersion()
	{
		return buffer[18];
	}
	
	public int getRezeroInfo()
	{
		return buffer[19];
	}
	
	public int getXCalibration()
	{
		return (int)createInt(buffer[20], buffer[21], buffer[22], (byte)0);
	}
	
	public int getYCalibration()
	{
		return (int)createInt(buffer[23], buffer[24], buffer[25], (byte)0);
	}
	
	public String getLocationName()
	{
		return Util.bytesToString(buffer, 26, 8);
	}
	
	public int[][] getData()
	{
		int samples = (getNumBytes() - 34) / (2 * 3);
		System.out.println("samples: " + samples);
		int[][] result = new int[samples][2];
		
		for (int i = 0; i < samples; i++)
		{
			result[i][0] = (int)createInt(buffer[34 + i * 6 + 2], buffer[34 + i * 6 + 1], buffer[34 + i * 6], (byte)0);
			result[i][1] = (int)createInt(buffer[34 + i * 6 + 5], buffer[34 + i * 6 + 4], buffer[34 + i * 6 + 3], (byte)0);
		}
		
		return result;
	}
	
	public double[][] getImportData()
	{
		int samples = (getNumBytes() - 34) / (2 * 3);
		double[][] result = new double[samples][5];
		int[][] data = getData();
		double t0 = Util.ewToJ2K(getStartTime());
//		double xCal = 1000 * VOLTS_PER_COUNT * (getXCalibration() - MAX_BIT / 2);
//		double yCal = 1000 * VOLTS_PER_COUNT * (getYCalibration() - MAX_BIT / 2);
//		double xCal = getXCalibration() * 5000 / MAX_BIT - 2500;
//		double yCal = getYCalibration() * 2.980232e-4 - 2499;
		SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		df.setTimeZone(TimeZone.getTimeZone("GMT-8:00"));
		for (int i = 0; i < samples; i++)
		{
			result[i][0] = t0 + i * getSecondsPerSample();
			result[i][1] = (double)data[i][0] * 5000 / MAX_BIT - 2500;
			result[i][2] = (double)data[i][1] * 5000 / MAX_BIT - 2500;
			result[i][3] = 1.0;
			result[i][4] = 1.0;
//			System.out.printf("%s %.2f %.2f %f %f\n", df.format(Util.j2KToDate(result[i][0])), result[i][1], result[i][2], result[i][3], result[i][4]);
		}
		
		return result;
	}
	
	public String toString()
	{
		return "numBytes=" + getNumBytes() + "\n" +
				"startTime=" + getStartTime() + "\n" +
				"temperature=" + getTemperature() + "\n" +
				"voltage=" + getVoltage() + "\n" +
				"heading=" + getHeading() + "\n" + 
				"secondsPerSample=" + getSecondsPerSample() + "\n" +
				"gain=" + getGain() + "\n" +
				"serialNumber=" + getSerialNumber() + "\n" +
				"version=" + getVersion() + "\n" +
				"rezeroInfo=" + getRezeroInfo() + "\n" + 
				"xCalibration=" + getXCalibration() + "\n" +
				"yCalibration=" + getYCalibration() + "\n" +
				"locationName=" + getLocationName() + "\n";
	}
}
