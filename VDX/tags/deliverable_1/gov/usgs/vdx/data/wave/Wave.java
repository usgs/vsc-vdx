package gov.usgs.vdx.data.wave;

import gov.usgs.math.Butterworth;
import gov.usgs.math.FFT;
import gov.usgs.math.Filter;
import gov.usgs.util.IntVector;
import gov.usgs.util.Util;
import gov.usgs.vdx.data.BinaryDataSet;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.List;
import java.util.TimeZone;

/**
 * <p>
 * <code>Wave</code> is a class that handles regularly sampled
 * time-series data like a seismic waveform. This class is well suited to higher
 * sampling rate data (>10Hz) with little or no gaps.
 * <p>
 * All data are stored as 32-bit signed integers. The start time, as in the
 * whole USGS Java codebase, is in j2ksec (decimal seconds since Jan 1, 2000).
 * 
 * $Log: not supported by cvs2svn $
 * Revision 1.6  2006/08/10 14:31:44  cervelli
 * Changed join().
 *
 * Revision 1.5  2006/04/08 01:27:11  dcervelli
 * Changed exportToText() to throw exceptions for bug #34.
 *
 * Revision 1.4  2006/04/08 01:00:17  dcervelli
 * Changed subset() for bug #84.
 *
 * Revision 1.3  2006/04/03 05:15:28  dcervelli
 * New join method for better Swarm monitor mode.
 *
 * Revision 1.2  2005/09/22 20:51:23  dcervelli
 * Fixed toSAC().
 *
 * Revision 1.1  2005/08/26 20:39:00  dcervelli
 * Initial avosouth commit.
 *
 * @author Dan Cervelli
 */
public class Wave implements BinaryDataSet, Comparable<Wave>, Cloneable
{
	/**
	 * A value that indicates that this sample is not an actual data sample.
	 */
	public static int NO_DATA = Integer.MIN_VALUE;

	/**
	 * The sample buffer. This is made public to allow quick and easy access to
	 * the raw samples.
	 */
	public int[] buffer;

	/**
	 * The start time of the block of samples.
	 */
	private double startTime;

	/**
	 * The sampling rate of the data.
	 */
	private double samplingRate;

	/**
	 * This value is set if <code>register()</code> is called. This is the
	 * difference between the actual time of the sample and the grid it is
	 * registered to.
	 */
	private double registrationOffset = Double.NaN;

	// These values are cached to improved performance
	private transient double mean = Double.NaN;
	private transient double rsam = Double.NaN;
	private transient int max = Integer.MIN_VALUE;
	private transient int min = Integer.MAX_VALUE;
	private transient int[] dataRange = null;

	/**
	 * Empty constructor.
	 */
	public Wave()
	{}

	/**
	 * Duplicates another <code>Wave</code> performing a deep copy of
	 * the samples array.
	 * 
	 * @param wave the source wave
	 */
	public Wave(Wave wave)
	{
		startTime = wave.startTime;
		samplingRate = wave.samplingRate;
		buffer = new int[wave.buffer.length];
		System.arraycopy(wave.buffer, 0, buffer, 0, wave.buffer.length);
	}

	/**
	 * Constructs a <code>Wave</code> from variables.
	 * 
	 * @param b the samples buffer
	 * @param st the start time
	 * @param sr the sampling rate
	 */
	public Wave(int[] b, double st, double sr)
	{
		buffer = b;
		startTime = st;
		samplingRate = sr;
	}

	/**
	 * Constructs a <code>Wave</code> from an array of bytes. This
	 * constructor is best used when transmitting <code>Waves</code>
	 * over a network.
	 * <p>
	 * The order of the binary variables is as follows: startTime, samplingRate,
	 * registrationOffset, number of samples (integer), and then the buffer of
	 * samples.
	 * 
	 * @param bb the byte buffer.
	 */
	public Wave(ByteBuffer bb)
	{
		fromBinary(bb);
	}

	/**
	 * Registers the starting time to the nearest even interval based on the
	 * wave's sampling rate. The offset between the original start time and the
	 * registered time is stored in <code>registrationOffset</code>.
	 */
	public void register()
	{
		double n = startTime;
		double m = 1 / samplingRate;

		double dif = n % m;
		if (dif >= m / 2)
			registrationOffset = (m - dif);
		else
			registrationOffset = -dif;

		startTime += registrationOffset;
	}

	/**
	 * Sets the registration offset.
	 * @param o the registration offset.
	 */
	public void setRegistrationOffset(double o)
	{
	    registrationOffset = o;
	}
	
	/**
	 * Determines if all of the samples are NO_DATA samples.
	 * 
	 * @return whether or not this consists entirely of NO_DATA samples
	 */
	public boolean isData()
	{
		for (int i = 0; i < buffer.length; i++)
			if (buffer[i] != NO_DATA)
				return true;

		return false;
	}

	/**
	 * Calculates the spectrogram of this <code>Wave</code>. This
	 * function has several variables that determines it's behavior. The primary
	 * parameter is <code>sampleSize</code>, which specifies how large the
	 * FFT bin size is. This unchecked value should be a power of 2. Next is
	 * <code>logPower</code>, which it whether or not the log of the power
	 * should be taken before the magnitude is put into the final array. The
	 * third parameter, <code>logFreq</code>, is reserved in case anyone
	 * wants to implement a log frequency function (noone asked for it and it
	 * seemed a bit silly so I didn't implement it). The last variable is
	 * <code>overlap</code> which is used to specify how much overlap (as a
	 * percentage) there should be in subsequent FFT bins. There is no input
	 * checking on this value so be careful what you ask for. In general, the
	 * higher the overlap value the 'smoother' the spectrogram will look at the
	 * cost of introducing artifacts. The returned array is the magnitude for
	 * the frequency range in each of the <code>sampleSize</code> sized bins.
	 * This array is best used as the input to an <code>ImageDataRenderer</code>.
	 * 
	 * @param sampleSize the number of samples for each FFT bin (must be a power
	 *            of 2)
	 * @param logPower whether or not to take the log of the power
	 * @param logFreq whether or not to take the log of the frequency (not
	 *            implemented)
	 * @param overlap the overlap as a percentage in FFT bins
	 * @return the magnitude array
	 */
	public double[][] toSpectrogram(int sampleSize, boolean logPower,
			boolean logFreq, double overlap)
	{
		int overlapSize = (int)(sampleSize * overlap);
		if (overlapSize >= sampleSize)
			overlapSize = sampleSize - 1;
		int xSize = (buffer.length / (sampleSize - overlapSize));
		int ySize = sampleSize / 2;

		double[][] powerBuf = new double[xSize][ySize];
		double[][] smallBuf = new double[sampleSize][2];
		double re, im, mag;
		int m = (int)Math.round(mean());
		for (int i = 0; i < xSize; i++)
		{
			int si = i * (sampleSize - overlapSize);
			for (int j = 0; j < sampleSize; j++)
			{
				if (si + j < buffer.length && buffer[si + j] != NO_DATA)
					smallBuf[j][0] = buffer[si + j];
				else
					smallBuf[j][0] = m;
				smallBuf[j][1] = 0;
			}
			FFT.fft(smallBuf);
			for (int j = 1; j < sampleSize / 2; j++)
			{
				re = smallBuf[j][0];
				im = smallBuf[j][1];
				mag = Math.sqrt(re * re + im * im);
				if (logPower)
					mag = Math.log(mag) / FFT.LOG10;
				powerBuf[i][j] = mag;
			}
		}
		return powerBuf;
	}

	/**
	 * Truncate internal data buffer
	 * @param s new size
	 */
	public void trunc(int s)
	{
		int[] buf = new int[s];
		System.arraycopy(buffer, 0, buf, 0, s);
		buffer = buf;
	}

	/**
	 * Subtract integer from every data value in the buffer
	 * @param m value to subtract
	 */
	public void subtract(int m)
	{
		for (int i = 0; i < buffer.length; i++)
			buffer[i] -= m;
	}

	/**
	 * Set sample rate
	 */
	public void setSamplingRate(double sr)
	{
		samplingRate = sr;
	}

	/**
	 * Set start time
	 */
	public void setStartTime(double st)
	{
		startTime = st;
	}

	/**
	 * Computes the FFT of the <code>Wave</code>. This function will
	 * perform the necessary zero-padding in order to make the samples buffer be
	 * a power of 2 in size.
	 * 
	 * @return the FFT (n rows x 2 column [real/imaginary]) array
	 * @see FFT
	 */
	public double[][] fft()
	{
		int n = buffer.length;
		int p2 = (int)Math.ceil(Math.log((double)n) / Math.log(2));
		int newSize = (int)Math.pow(2, p2);
		double[][] buf = new double[newSize][2];
		int m = (int)Math.round(mean());

		for (int i = 0; i < n; i++)
			if (buffer[i] != NO_DATA)
				buf[i][0] = buffer[i];

		for (int i = n; i < newSize; i++)
			buf[i][0] = m;

		FFT.fft(buf);

		return buf;
	}

	/**
	 * Invalidates the cached statistics.
	 */
	public void invalidateStatistics()
	{
		mean = Double.NaN;
		rsam = Double.NaN;
		max = Integer.MIN_VALUE;
		min = Integer.MAX_VALUE;
	}

	/**
	 * Scan data and compute statistic
	 */
	private void deriveStatistics()
	{
		if (buffer == null || buffer.length == 0)
		{
			mean = 0;
			rsam = 0;
			max = 0;
			min = 0;
			return;
		}
		int noDatas = 0;
		long sum = 0;
		long rs = 0;
		for (int i = 0; i < buffer.length; i++)
		{
			int d = buffer[i];
			if (d != NO_DATA)
			{
				sum += d;
				rs += Math.abs(d);
				min = Math.min(min, d);
				max = Math.max(max, d);
			}
			else
				noDatas++;
		}

		mean = (double)sum / (double)(buffer.length - noDatas);
		rsam = (double)rs / (double)(buffer.length - noDatas);
		dataRange = new int[] { min, max };
	}

	/**
	 * Gets the mean or bias of the samples. Ignores NO_DATA samples.
	 * 
	 * @return the mean or bias
	 */
	public double mean()
	{
		if (Double.isNaN(mean))
			deriveStatistics();

		return mean;
	}

	/**
	 * Gets the maximum value of the samples. Ignores NO_DATA samples.
	 * 
	 * @return the maximum value
	 */
	public int max()
	{
		if (max == Integer.MIN_VALUE)
			deriveStatistics();

		return max;
	}

	/**
	 * Gets the minimum value of the samples. Ignores NO_DATA samples.
	 * 
	 * @return the minimum value
	 */
	public int min()
	{
		if (min == Integer.MAX_VALUE)
			deriveStatistics();

		return min;
	}

	/**
	 * Gets the RSAM of the samples. Ignores NO_DATA samples.
	 * 
	 * @return the RSAM value
	 */
	public double rsam()
	{
		if (Double.isNaN(rsam))
			deriveStatistics();

		return rsam;
	}

	/**
	 * Decimates the wave by taking every nth sample.
	 * 
	 * @param factor the value of n
	 */
	public void decimate(int factor)
	{
		int[] buf = new int[samples() / factor];
		for (int i = 0; i < samples() / factor; i++)
			buf[i] = buffer[i * factor];

		buffer = buf;
		samplingRate /= factor;
	}

	/**
	 * Gets the number of samples in the buffer.
	 * 
	 * @return the number of samples
	 */
	public int samples()
	{
		return buffer.length;
	}

	/**
	 * Converts the starttime from Earthworm time to J2K
	 */
	public void convertToJ2K()
	{
		startTime = Util.ewToJ2K(startTime);
	}

	/**
	 * Gets the sampling rate.
	 * 
	 * @return the sampling rate
	 */
	public double getSamplingRate()
	{
		return samplingRate;
	}

	/**
	 * Gets the start time of the samples.
	 * 
	 * @return the start time
	 */
	public double getStartTime()
	{
		return startTime;
	}

	/**
	 * Gets the end time for this wave. This is equals to the samples * period,
	 * so it reports the time AFTER the last sample, not FOR the last sample.
	 * 
	 * @return the end time
	 */
	public double getEndTime()
	{
		return startTime + (double)samples() * 1.0 / samplingRate;
	}

	/**
	 * Gets the registration offset of this wave.
	 * 
	 * @return the registration offset
	 */
	public double getRegistrationOffset()
	{
		return registrationOffset;

	}

	/**
	 * Gets the minimum and maximum values of this wave. This is safe to use in
	 * a loop -- a new int array is not generated on each call.
	 * 
	 * @return an array with minimum and maximum value (in that order)
	 */
	public int[] getDataRange()
	{
		if (min == Integer.MAX_VALUE)
			deriveStatistics();

		return dataRange;
	}

	/**
	 * Determines whether or not this wave is adjacent to another wave in time.
	 * 
	 * @param wave the test wave
	 * @return whether or not these waves are adjacent
	 */
	public boolean adjacent(Wave wave)
	{
		if (getEndTime() == wave.getStartTime())
			return true;

		if (getStartTime() == wave.getEndTime())
			return true;

		return false;
	}

	/**
	 * Determines whether or not this wave overlaps the other wave in time. This
	 * does not cover cases where they are adjacent to each other, use
	 * <code>adjacent()</code> for that.
	 * 
	 * @param wave the test wave
	 * @return whether or not these waves overlap
	 */
	public boolean overlaps(Wave wave)
	{
		// obviously this could be compressed to one line, but this is readable:
		// either the new wave is completely right of, completely left of or
		// overlapping the old wave
		if (getEndTime() <= wave.getStartTime())
			return false;

		if (getStartTime() >= wave.getEndTime())
			return false;

		return true;
	}

	/**
	 * Determines whether or not this wave overlaps a given time interval. This
	 * does not cover cases where they are adjacent to each other, use
	 * <code>adjacent()</code> for that.
	 * 
	 * @param t1 the start time of the interval
	 * @param t2 the end time of the interval
	 * @return whether or not these waves overlap
	 */
	public boolean overlaps(double t1, double t2)
	{

		if (getEndTime() <= t1)
			return false;

		if (getStartTime() >= t2)
			return false;

		return true;
	}

	/**
	 * Splits this wave in half, creating two separate waves.
	 * 
	 * @return an array of two waves
	 */
	public Wave[] split()
	{
		Wave sw1 = new Wave();
		Wave sw2 = new Wave();
		sw1.startTime = startTime;
		sw1.samplingRate = samplingRate;
		int length1 = buffer.length / 2;
		sw1.buffer = new int[length1];
		System.arraycopy(buffer, 0, sw1.buffer, 0, length1);

		sw2.startTime = startTime + (double)length1 * (1 / samplingRate);
		sw2.samplingRate = samplingRate;
		int length2 = buffer.length / 2 + (buffer.length % 2);
		sw2.buffer = new int[length2];
		System.arraycopy(buffer, length1, sw2.buffer, 0, length2);

		return new Wave[] { sw1, sw2 };
	}
	
	/**
	 * Splits this wave into a list of smaller waves with a maximum number of 
	 * samples as specified.  Used for breaking a wave into TRACEBUF-sized
	 * pieces.
	 *  
	 * @param maxSamples
	 * @return a list of the split up waves.
	 */
	public List<Wave> split(int maxSamples)
	{
		ArrayList<Wave> list = new ArrayList<Wave>(buffer.length / maxSamples + 2);
		double ct = startTime;
		int j = 0;
		while (j < buffer.length)
		{
			int numSamples = Math.min(maxSamples, buffer.length - j);
			Wave sw = new Wave();
			sw.startTime = ct;
			sw.samplingRate = samplingRate;
			sw.buffer = new int[numSamples];
			System.arraycopy(buffer, j, sw.buffer, 0, numSamples);
			ct += (double)numSamples * (1 / samplingRate);
			j += numSamples;
			list.add(sw);
		}
		return list;
	}

	/**
	 * Gets a strict subset of this wave. This function should have more bounds
	 * checking. It will fail if you ask for a portion of wave that is not
	 * contained in the original wave.
	 * 
	 * @param t1 the start time of the subset
	 * @param t2 the end time of the subset
	 * @return the subset wave
	 */
	public Wave subset(double t1, double t2)
	{
		if (t1 < getStartTime() || t2 > getEndTime() || t2 < t1)
			return this;

		int samples = (int)Math.floor((t2 - t1) * samplingRate);
		int offset = (int)Math.floor((t1 - startTime) * samplingRate);
		Wave sw = new Wave();
		sw.startTime = t1;
		sw.samplingRate = samplingRate;
		sw.registrationOffset = registrationOffset;
		sw.buffer = new int[samples];
		System.arraycopy(buffer, offset, sw.buffer, 0, samples);
		return sw;
	}
	
	/**
	 * Expiremental.  For use with Winston static importers.
	 * TODO: handle erase a chunk inside the existing wave
	 * @param t1
	 * @param t2
	 */
	public void erase(double t1, double t2)
	{
		if (t2 < getStartTime() || t1 > getEndTime())
			return;  // nothing to erase
		
		if (t1 >= getStartTime() && t2 <= getEndTime())
			return;  // erase the middle -- unhandled
		
		if (t1 <= getStartTime() && t2 >= getEndTime())
		{
			// erase the whole wave
			buffer = null;
			startTime = Double.NaN;
			samplingRate = Double.NaN;
		}
		
		if (t2 > getStartTime() && t2 <= getEndTime())
		{
			// erase left side
			int size = (int)Math.round((t2 - getStartTime()) * samplingRate);
			int[] buf = new int[buffer.length - size];
			System.arraycopy(buffer, size, buf, 0, buffer.length - size);
			buffer = buf;
			startTime = t2;
		}
		
		if (t1 >= getStartTime() && t1 <= getEndTime())
		{
			// erase right side
			int size = (int)Math.round((getEndTime() - t1) * samplingRate);
			int[] buf = new int[buffer.length - size];
//			System.out.println("new size: " + buf.length);
			System.arraycopy(buffer, 0, buf, 0, buffer.length - size);
			buffer = buf;
		}
	}

	/**
	 * <p>
	 * Combines this wave with another overlapping one. This function will
	 * return a combination of the two waves. This modifies one of the waves and
	 * returns it. Neither wave is guaranteed to be safe from modification.
	 * <p>
	 * This is only intended to work on overlapping waves (as checked by the
	 * <code>overlap()</code> function.
	 * 
	 * @param wave
	 * @return
	 */
	public Wave combine(Wave wave)
	{
		if (samplingRate != wave.getSamplingRate())
			return null;

		// if (!overlaps(wave))
		//		return null;

		// other wave dominates this wave
		if (startTime >= wave.getStartTime()
				&& getEndTime() <= wave.getEndTime())
			return wave;

		// this wave dominates other wave
		if (startTime <= wave.getStartTime()
				&& getEndTime() >= wave.getEndTime())
			return this;

		// this wave is left of other wave
		if (startTime <= wave.getStartTime())
		{
//									System.out.println("left join");
			//						System.out.println(((wave.getEndTime() - startTime) *
			// samplingRate));
			//			int[] newbuf = new int[(int) ((wave.getEndTime() - startTime) *
			// samplingRate)];
			int[] newbuf = new int[(int)Math
					.round((wave.getEndTime() - startTime) * samplingRate)];
			//						System.out.println("newbuf.length " + newbuf.length);
			System.arraycopy(buffer, 0, newbuf, 0, buffer.length);
			//			System.out.println("delta et*sr: " + ((wave.getEndTime() -
			// getEndTime()) * samplingRate));
			int len = (int)Math.round((wave.getEndTime() - getEndTime())
					* samplingRate);
			//						System.out.println("len " + len);

			//			System.out.println("this.et-wave.st * sr: " + (getEndTime() -
			// wave.getStartTime()) * samplingRate);
			int i = (int)((getEndTime() - wave.getStartTime()) * samplingRate);

			//			Commented these out after implementing registration
			//			was causing little dropouts to 0.
			//						System.out.println("i " + i);
			//			if (buffer.length + len >= newbuf.length && len != 0)
			//			{
			//				System.out.println("dec");
			//				len--;
			//			}

			// ArrayIndexOutOfBounds here
			//			System.out.println("blen " + buffer.length);
			//			System.out.println("this: " + this);
			//			System.out.println("wave: " + wave);
			System.arraycopy(wave.buffer, i, newbuf, buffer.length, len);
			this.buffer = newbuf;
			return this;
		}

		// this wave is right of other wave
		if (wave.getStartTime() <= startTime)
		{
//		    System.out.println(wave);
//		    System.out.println(this);
//		    System.out.println("right join");
//			System.out.println("new startTime: " + wave.startTime);
//			System.out.println("new endTime: " + getEndTime());
			double dt = getEndTime() - wave.startTime;
			//dt += Util.register(dt, 1 / samplingRate);
//			System.out.println("dt: " + dt);
//			System.out.println("reg: " + Util.register(dt, 1 / samplingRate));
//			int[] newbuf = new int[(int)Math.round((getEndTime() - wave.startTime) * samplingRate)];
			int[] newbuf = new int[(int)Math.round((dt) * samplingRate)];
//			System.out.println("new length: " + newbuf.length);
			System.arraycopy(wave.buffer, 0, newbuf, 0, wave.buffer.length);
			//			int len = (int) ((getEndTime() - wave.getEndTime()) *
			// samplingRate);
			int len = (int)Math.round((getEndTime() - wave.getEndTime())
					* samplingRate);
			//			System.out.println("len " + len);
			int i = (int)((wave.getEndTime() - getStartTime()) * samplingRate);
			//			System.out.println("i " + i);
			//			if (wave.buffer.length + len >= newbuf.length && len != 0)
			//			{
			//				System.out.println("dec");
			//				len--;
			//			}
			System.arraycopy(buffer, i, newbuf, wave.buffer.length, len);
			this.buffer = newbuf;
			this.startTime = wave.startTime;
			return this;
		}

		//System.out.println("unknown case");
		return null;
	}

	/**
	 * Joins together a list of waves into one large wave. The list must be
	 * sorted in time-ascending order of start time. Technically, as long as the
	 * first wave is the earliest and the last wave ends the latest, this
	 * function will work. This function will happily create a wave with large
	 * gaps in it as well.
	 * 
	 * @param waves the list of <code>Wave</code> s
	 * @return the new joined wave
	 */
	public static Wave join(List<Wave> waves)
	{
		if (waves == null || waves.size() == 0)
			return null;

		if (waves.size() == 1)
			return waves.get(0);

		double mint = 1E300;
		double maxt = -1E300;
		double sr = -1;
		for (Wave sw : waves)
		{
			mint = Math.min(mint, sw.getStartTime());
			maxt = Math.max(maxt, sw.getEndTime());
			sr = sw.getSamplingRate();
		}
		
//		Wave wv0 = waves.get(0);
//		Wave wvn = waves.get(waves.size() - 1);

//		int samples = (int)((wvn.getEndTime() - wv0.getStartTime()) * wv0
//				.getSamplingRate());
		int samples = (int)((maxt - mint) * sr);

		int[] buffer = new int[samples + 1];
		Arrays.fill(buffer, NO_DATA);

		for (Wave sw : waves)
		{
//			System.out.println(sw);
//			int i = (int)Math.round((sw.getStartTime() - wv0.getStartTime())
//					* wv0.getSamplingRate());
			int i = (int)Math.round((sw.getStartTime() - mint) * sr);
//			System.out.println("join: " + i + " " + buffer.length + " " + sw.buffer.length);
			System.arraycopy(sw.buffer, 0, buffer, i, sw.buffer.length);
		}

		return new Wave(buffer, mint, sr);//, wv0.getSamplingRate());
	}
	
	/**
	 * Joins together a list of waves into one large wave.  Forces extent of 
	 * wave from arguments so sort order does not matter.
	 * 
	 * @param waves the list of <code>Wave</code> s
	 * @return the new joined wave
	 */
	public static Wave join(List<Wave> waves, double t1, double t2)
	{
		if (waves == null || waves.size() == 0)
			return null;

//		if (waves.size() == 1)
//			return waves.get(0);

		Wave wv0 = waves.get(0);

		int samples = (int)((t2 - t1) * wv0.getSamplingRate());

		int[] buffer = new int[samples + 1];
		Arrays.fill(buffer, NO_DATA);

		for (Wave sw : waves)
		{
			int i = (int)Math.round((sw.getStartTime() - t1) * wv0.getSamplingRate());
			System.arraycopy(sw.buffer, 0, buffer, i, sw.buffer.length);
		}

		return new Wave(buffer, t1, wv0.getSamplingRate());
	}

	/**
	 * Gets an estimate for the size in RAM of this <code>Wave</code>.
	 * It doesn't count the size of the header information, assuming instead
	 * that it is insignificant compared to the size of the buffer.
	 * 
	 * @return the approximate size in RAM of this wave
	 */
	public int getMemorySize()
	{
		if (buffer == null)
			return 0;
		else
			return buffer.length * 4;
	}

	/**
	 * Exports this <code>Wave</code> to a simple text file. This
	 * function assumes that the time is in J2K seconds and converts it into
	 * Unix milliseconds. The output is a simple 2-D rectangular text file,
	 * first column time, second column sample.
	 * 
	 * @param fn the output filename
	 */
	public void exportToText(String fn) throws IOException
	{
		PrintWriter out = new PrintWriter(new BufferedWriter(
				new FileWriter(fn)));
		double ct = startTime + 946728000;
		double ts = 1 / samplingRate;
		for (int i = 0; i < buffer.length; i++)
		{
			out.println(Math.round(ct * 1000) + " " + buffer[i]);
			ct += ts;
		}
		out.close();
	}

	/**
	 * Creates a SAC object from this wave.
	 * 
	 * @return the SAC object
	 */
	/*
	public SAC toSAC()
	{
	    SAC sac = new SAC();
	    sac.npts = buffer.length;
	    Calendar cal = Calendar.getInstance(TimeZone.getTimeZone("GMT"));
	    cal.setTime(Util.j2KToDate(startTime));
	    sac.nzyear = cal.get(Calendar.YEAR);
	    sac.nzjday = cal.get(Calendar.DAY_OF_YEAR);
	    sac.nzhour = cal.get(Calendar.HOUR_OF_DAY);
	    sac.nzmin = cal.get(Calendar.MINUTE);
	    sac.nzsec = cal.get(Calendar.SECOND);
	    sac.nzmsec = cal.get(Calendar.MILLISECOND);
	    sac.delta = (float)(1 / samplingRate);
	    sac.y = new float[buffer.length];
	    for (int i = 0; i < buffer.length; i++)
	        sac.y[i] = buffer[i];
	    return sac;
	}
	*/
	
	/**
	 * Imports from a simple text file like one generated from
	 * <code>exportToText()</code>. First column is assumed to be Unix
	 * millisecond time, second column sample.
	 * 
	 * TODO: throw exception
	 * TODO: get rid of IntVector
	 * @param fn the import filename
	 * @return the wave
	 */
	public static Wave importFromText(String fn)
	{
		try
		{
//			File f = new File(fn);
			//long size = f.length();
			IntVector iv = new IntVector(5000, 10000);
			BufferedReader in = new BufferedReader(new FileReader(fn));
			Wave sw = new Wave();
			String firstLine = in.readLine();
			sw.startTime = Double.parseDouble(firstLine.substring(0, firstLine
					.indexOf(" ")));
			iv.add(Integer.parseInt(firstLine
					.substring(firstLine.indexOf(" ") + 1)));
			String secondLine = in.readLine();
			double nt = Double.parseDouble(secondLine.substring(0, secondLine
					.indexOf(" ")));
			sw.samplingRate = 1 / ((nt - sw.startTime) / 1000);
			String line = secondLine;
			do
			{
				iv.add(Integer.parseInt(line.substring(line.indexOf(" ") + 1)));
			}
			while ((line = in.readLine()) != null);

			sw.startTime /= 1000;
			sw.startTime -= 946728000;
			sw.buffer = iv.getResizedInts();
			in.close();
			return sw;
		}
		catch (Exception e)
		{
//			e.printStackTrace();
		}
		return null;
	}

	/**
	 * Integrates the wave. This functions takes the cumulative sum (mean
	 * removed) of the wave at each sample, dividing by the sampling rate.
	 * 
	 * @return an array of the sum at each sample
	 */
	public double[] integrate()
	{
		double[] d = new double[this.buffer.length];
		double period = 1 / samplingRate;
		double mean = mean();
		double sum = 0;
		for (int i = 0; i < this.buffer.length; i++)
		{
			sum += ((double)buffer[i] - mean);
			d[i] = sum * period;
		}

		return d;
	}

	public double[] removeMean()
	{
		double[] d = new double[this.buffer.length];
		double mean = mean();
		for (int i = 0; i < this.buffer.length; i++)
			d[i] = (double)buffer[i] - mean;
		
		return d;
	}
	/**
	 * Filter data
	 * @param bw Butterworth filter to apply
	 */
	public void filter(Butterworth bw, boolean zeroPhaseShift)
	{
		double mean = mean();
		
		double[] dBuf = new double[buffer.length + (int)(buffer.length * 0.5)];
		Arrays.fill(dBuf, mean);
		int trueStart = (int)(buffer.length * 0.25);
		for (int i = 0; i < buffer.length; i++)
		{
			if (buffer[i] != Wave.NO_DATA)
				dBuf[i + trueStart] = buffer[i];
		}

		bw.setSamplingRate(getSamplingRate());
		bw.create();
		Filter.filter(dBuf, bw.getSize(), bw.getXCoeffs(), bw.getYCoeffs(), bw.getGain(), 0, 0);
		if (zeroPhaseShift)
		{
			double[] dBuf2 = new double[dBuf.length];
			for (int i = 0, j = dBuf.length - 1; i < dBuf.length; i++, j--)
				dBuf2[j] = dBuf[i];	
			
			Filter.filter(dBuf2, bw.getSize(), bw.getXCoeffs(), bw.getYCoeffs(), bw.getGain(), 0, 0);
			
			for (int i = 0, j = dBuf2.length - 1 - trueStart; i < buffer.length; i++, j--)
				buffer[i] = (int)Math.round(dBuf2[j]);
		}
		else
		{
			for (int i = 0; i < buffer.length; i++)
				buffer[i] = (int)Math.round(dBuf[i + trueStart]);
		}
		invalidateStatistics();
	}
	
	/**
	 * Creates a <code>ByteBuffer</code> that wholly contains this
	 * <code>Wave</code>. This is used before sending a
	 * <code>Wave</code> over a network (or potentially into a file).
	 * The <code>ByteBuffer</code> needs to be flipped before it can fed
	 * somewhere.
	 * 
	 * @return the bytes that make up the wave
	 */
	public ByteBuffer toBinary()
	{
		ByteBuffer bb = ByteBuffer.allocate(28 + 4 * buffer.length);
		bb.putDouble(startTime);
		bb.putDouble(samplingRate);
		bb.putDouble(registrationOffset);
		bb.putInt(buffer.length);
		for (int i = 0; i < buffer.length; i++)
			bb.putInt(buffer[i]);

		return bb;
	}
	
	/**
	 * Restore Wave content from ByteBuffer
	 */
	public void fromBinary(ByteBuffer bb)
	{
		startTime = bb.getDouble();
		samplingRate = bb.getDouble();
		registrationOffset = bb.getDouble();
		int length = bb.getInt();
		buffer = new int[length];

		for (int i = 0; i < length; i++)
			buffer[i] = bb.getInt();
	}

	/**
	 * Gets a <code>String</code> summary of this wave.
	 * 
	 * @return the summary
	 */
	public String toString()
	{
		return "Wave: startTime=" + startTime + ", endTime="
				+ getEndTime() + ", samplingRate=" + samplingRate
				+ ", samples=" + buffer.length 
				+ "\nstartDate=" + Util.j2KToDateString(startTime) 
			 	+ "\nendDate=" + Util.j2KToDateString(getEndTime());
	}
	
	/**
	 * Creates a SAC object from this wave.
	 * 
	 * @return the SAC object
	 */
	public SAC toSAC()
	{
	    SAC sac = new SAC();
	    sac.npts = buffer.length;
	    Calendar cal = Calendar.getInstance(TimeZone.getTimeZone("GMT"));
	    cal.setTime(Util.j2KToDate(startTime));
	    sac.nzyear = cal.get(Calendar.YEAR);
	    sac.nzjday = cal.get(Calendar.DAY_OF_YEAR);
	    sac.nzhour = cal.get(Calendar.HOUR_OF_DAY);
	    sac.nzmin = cal.get(Calendar.MINUTE);
	    sac.nzsec = cal.get(Calendar.SECOND);
	    sac.nzmsec = cal.get(Calendar.MILLISECOND);
	    sac.delta = (float)(1 / samplingRate);
	    sac.y = new float[buffer.length];
	    for (int i = 0; i < buffer.length; i++)
	        sac.y[i] = buffer[i];
	    sac.b = 0;
	    sac.e = (float)buffer.length * sac.delta;
	    sac.fmt = 2.0f;
	    sac.iftype = SAC.ITIME;
	    sac.idep = SAC.IUNKN;
	    sac.iztype = SAC.IB;
	    sac.leven = SAC.TRUE;
	    sac.lpspol = SAC.TRUE;
	    sac.lovrok = SAC.TRUE;
	    sac.lcalda = SAC.TRUE;
	    sac.kevnm = SAC.STRING8_UNDEF;
	    sac.ko = "origin";
	    
	    return sac;
	}

	/**
	 * Compare waves by start time
	 */
	public int compareTo(Wave o) 
	{
		return (int)Math.round(getStartTime() - o.getStartTime());
	}
	
	public Wave clone() {
		Wave w = new Wave(this);
		
		return w;
	}
}