package gov.usgs.vdx.data.wave;

import gov.usgs.math.FFT;

/**
 * 
 * $Log: not supported by cvs2svn $
 * @author Dan Cervelli
 */
public class SliceWave
{
	private double startTime;
	private double samplingRate;
	private double registrationOffset;
	
	private int[] buffer;
	private int position;
	private int limit;
	
	private int readPosition;
	
	// These values are cached to improved performance
	private transient double mean = Double.NaN;
	private transient double rsam = Double.NaN;
	private transient int max = Integer.MIN_VALUE;
	private transient int min = Integer.MAX_VALUE;
	private transient int[] dataRange = null;
	
	public SliceWave(Wave sw)
	{
		startTime = sw.getStartTime();
		samplingRate = sw.getSamplingRate();
		registrationOffset = sw.getRegistrationOffset();
		
		buffer = sw.buffer;
		position = 0; 
		limit = buffer.length;
	}
	
	public double getRegistrationOffset()
	{
		return registrationOffset;
	}
	
	public int[] getDataRange()
	{
		if (dataRange == null)
			deriveStatistics();
		
		return dataRange;
	}
	
	public int samples()
	{
		return limit - position;
	}
	
	public double getSamplingRate()
	{
		return samplingRate;
	}
	
	public double getStartTime()
	{
		return startTime + position * (1 / samplingRate);
	}
	
	public double getEndTime()
	{
		return startTime + limit * (1 / samplingRate);
	}

	private double getTrueEndTime()
	{
		return startTime + buffer.length * (1 / samplingRate);
	}
	
	public void setSlice(double t1, double t2)
	{
		if (t1 < startTime || t2 > getTrueEndTime() || t1 >= t2)
			return;
	
		invalidateStatistics();
		
		position = (int)Math.round((t1 - startTime) * samplingRate);
		limit = position + (int)Math.round((t2 - t1) * samplingRate);
		
		if (limit > buffer.length)
			limit = buffer.length;
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
		for (int i = position; i < limit; i++)
		{
			int d = buffer[i];
			if (d != Wave.NO_DATA)
			{
				sum += d;
				rs += Math.abs(d);
				min = Math.min(min, d);
				max = Math.max(max, d);
			}
			else
				noDatas++;
		}
		
		mean = (double)sum / (double)(samples() - noDatas);
		rsam = (double)rs / (double)(samples() - noDatas);
		dataRange = new int[] {min, max};
	}

	/**
	 * Gets the mean or bias of the samples.  Ignores NO_DATA samples.
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
	 * Gets the maximum value of the samples.  Ignores NO_DATA samples.
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
	 * Gets the minimum value of the samples.  Ignores NO_DATA samples.
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
	 * Gets the RSAM of the samples.  Ignores NO_DATA samples.
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
	 * Determines if all of the samples are NO_DATA samples.
	 * @return whether or not this consists entirely of NO_DATA samples
	 */
	public boolean isData()
	{
		for (int i = 0; i < buffer.length; i++)
			if (buffer[i] != Wave.NO_DATA)
				return true;
			
		return false;
	}

	/**
	 * Calculates the spectrogram of this <code>Wave</code>.  This 
	 * function has several variables that determines it's behavior.  The 
	 * primary parameter is <code>sampleSize</code>, which specifies how large
	 * the FFT bin size is.  This unchecked value should be a power of 2. Next
	 * is <code>logPower</code>, which it whether or not the log of the power 
	 * should be taken before the magnitude is put into the final array.  The
	 * third parameter, <code>logFreq</code>, is reserved in case anyone
	 * wants to implement a log frequency function (noone asked for it and
	 * it seemed a bit silly so I didn't implement it).  The last variable is
	 * <code>overlap</code> which is used to specify how much overlap (as a
	 * percentage) there should be in subsequent FFT bins.  There is no input
	 * checking on this value so be careful what you ask for.  In general, 
	 * the higher the overlap value the 'smoother' the spectrogram will look at
	 * the cost of introducing artifacts.
	 * 
	 * The returned array is the magnitude for the frequency range in each of 
	 * the <code>sampleSize</code> sized bins.  This array is best used as
	 * the input to an <code>ImageDataRenderer</code>.
	 * 
	 * @param sampleSize the number of samples for each FFT bin (must be a power
	 * of 2)
	 * @param logPower whether or not to take the log of the power
	 * @param logFreq whether or not to take the log of the frequency (not
	 * implemented)
	 * @param overlap the overlap as a percentage in FFT bins 
	 * @return the magnitude array
	 */
	public double[][] toSpectrogram(int sampleSize, boolean logPower, 
			boolean logFreq, double overlap)
	{
		int overlapSize = (int)(sampleSize * overlap);
		if (overlapSize >= sampleSize)
			overlapSize = sampleSize - 1;
		int xSize = (samples() / (sampleSize - overlapSize));
		int ySize = sampleSize / 2;
		
		double[][] powerBuf = new double[xSize][ySize];
		double[][] smallBuf = new double[sampleSize][2];
		double re, im, mag;
//			int m = (int)Math.round(mean());
		int m = 0;
		for (int i = 0; i < xSize; i++)
		{
			int si = i * (sampleSize - overlapSize);
			for (int j = 0; j < sampleSize; j++)
			{
				if (si + j < samples() && buffer[position + si + j] != Wave.NO_DATA)
					smallBuf[j][0] = buffer[position + si + j];
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
	 * Computes the FFT of the <code>Wave</code>.  This function will
	 * perform the necessary zero-padding in order to make the samples buffer
	 * be an power of 2 in size.
	 *  
	 * @return the FFT (n rows x 2 column [real/imaginary]) array
	 * @see FFT
	 */
	public double[][] fft()
	{
		int n = samples();
		int p2 = (int)Math.ceil(Math.log((double)n) / Math.log(2));
		int newSize = (int)Math.pow(2, p2);
		double[][] buf = new double[newSize][2];
		int m = (int)Math.round(mean());
		
		for (int i = 0; i < newSize; i++)
				buf[i][0] = m;
		reset();
		int i = 0;
		while (hasNext())
		{
			if (buffer[i] != Wave.NO_DATA)
				buf[i][0] = next();
			else
				buf[i][0] = m;
			
			i++;
		}
	
		FFT.fft(buf);
	
		return buf;
	}
	
	public void reset()
	{
		readPosition = position;
	}
	
	public boolean hasNext()
	{
		return readPosition < limit;
	}
	
	public int next()
	{
		return buffer[readPosition++];
	}
	
}
