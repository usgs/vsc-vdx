package gov.usgs.vdx.data.wave;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;

import gov.usgs.math.FFT;
import gov.usgs.util.Util;

/**
 * Represents slice - continuous part of wave time series.
 * Contain computed and cached statistics about wave series zone.
 * 
 * TODO: return DoubleMatrix2D from FFT()
 * 
 * $Log: not supported by cvs2svn $
 * Revision 1.7  2012/01/04 02:12:29  pcervelli
 * Changed toSpectrogram method to use the Spectrogram class.
 * 
 * Revision 1.6  2005/12/28 02:12:29  tparker
 * Add toCSV method to support raw data export
 *
 * Revision 1.5  2005/12/16 23:30:47  dcervelli
 * Fixed min() and max() [was breaking remove bias in Swarm].
 *
 * Revision 1.4  2005/09/04 15:45:35  dcervelli
 * Uses faster 1-d FFT.
 *
 * Revision 1.3  2005/09/02 16:19:35  dcervelli
 * Added getWave().
 *
 * Revision 1.2  2005/09/01 00:29:42  dcervelli
 * Major refactor.
 *
 * Revision 1.1  2005/08/26 20:39:00  dcervelli
 * Initial avosouth commit.
 *
 * @author Dan Cervelli
 */
public class SliceWave
{
	private Wave source;
	
	private int position;
	private int limit;
	private int readPosition;
	
	// These values are cached to improved performance
	private transient double mean = Double.NaN;
	private transient double rsam = Double.NaN;
	private transient double max = -1E300;
	private transient double min = 1E300;
	private transient double[] dataRange = null;
	
	/**
	 * Constructor
	 * @param sw Time series
	 */
	public SliceWave(Wave sw)
	{
		source = sw;
		position = 0; 
		limit = source.buffer.length;
	}
	
	/**
	 * Getter for time series
	 * @return wave
	 */
	public Wave getWave()
	{
		return source;
	}
	
	/**
	 * Get data range - max and min slice data limits
	 * @return max and min slice data limits
	 */
	public double[] getDataRange()
	{
		if (dataRange == null)
			deriveStatistics();
		
		return dataRange;
	}
	
	/**
	 * Get samples count in represented slice
	 * @return samples count
	 */
	public int samples()
	{
		return limit - position;
	}
	
	/**
	 * Get Nyquist frequency
	 * @return Nyquist frequency
	 */
	public double getNyquist()
	{
		return source.getNyquist();
	}
	
	/**
	 * Get sample rate
	 * @return sample rate
	 */
	public double getSamplingRate()
	{
		return source.getSamplingRate();
	}
	
	/**
	 * Get slice start time
	 * @return start time
	 */
	public double getStartTime()
	{
		return source.getStartTime() + position * (1 / getSamplingRate());
	}
	
	/**
	 * Get slice end time
	 * @return end time
	 */
	public double getEndTime()
	{
		return source.getStartTime() + limit * (1 / getSamplingRate());
	}

	/**
	 * Get wave series end time
	 * @return end time
	 */
	private double getTrueEndTime()
	{
		return getStartTime() + source.buffer.length * (1 / getSamplingRate());
	}
	
	/**
	 * Set time limits to define slice zone, compute statistics
	 * @param t1 start time
	 * @param t2 end time
	 */
	public void setSlice(double t1, double t2)
	{
		if (t1 < source.getStartTime() || t2 > getTrueEndTime() || t1 >= t2)
			return;
	
		invalidateStatistics();
		
		position = (int)Math.round((t1 - source.getStartTime()) * getSamplingRate());
		limit = position + (int)Math.round((t2 - t1) * getSamplingRate());
		
		if (limit > source.buffer.length)
			limit = source.buffer.length;
	}
	
	/**
	 * Invalidates the cached statistics.
	 */
	public void invalidateStatistics()
	{
		mean = Double.NaN;
		rsam = Double.NaN;
		max = -1E300;
		min = 1E300;
	}
	 
	/**
	 * Compute slice statistics
	 */
	private void deriveStatistics()
	{
		if (source.buffer == null || source.buffer.length == 0)
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
			int d = source.buffer[i];
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
		dataRange = new double[] {min, max};
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
	public double max()
	{
		if (max == -1E300)
			deriveStatistics();
			
		return max;	
	}
	
	/**
	 * Gets the minimum value of the samples.  Ignores NO_DATA samples.
	 * 
	 * @return the minimum value
	 */
	public double min()
	{
		if (min == 1E300)
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
		for (int i = 0; i < source.buffer.length; i++)
			if (source.buffer[i] != Wave.NO_DATA)
				return true;
			
		return false;
	}

	/**
	 * Calculates the spectrogram of this <code>Wave</code>. 
	 * 
	 * @param binSize the number of samples for per window
	 * @param nfft the length of the FFT (0 = auto)  
	 * @param logPower whether or not to take the log of the power
	 * @param overlap the overlap in samples 
	 * @return the magnitude array
	 */
	
  public double[][] toSpectrogram(int binSize, int nfft, boolean logPower, int overlap)
	{		
		double[] signal;

		signal = new double[source.buffer.length];
		double mu = source.mean();
		for (int i = 0; i < source.buffer.length; i++)
			if (source.buffer[i] == Wave.NO_DATA)
				signal[i] = mu;
			else
				signal[i] = (double)source.buffer[i];
		
		int samplingRate = (int)source.getSamplingRate();
		
		// if nfft equals zero, then set it automatically
		
		if (nfft==0) {
			if (samplingRate<100)
				nfft = binSize;
			else
				nfft = 2 * binSize;
		}
		
		Spectrogram spectrogram = new Spectrogram(signal, samplingRate, nfft, binSize, overlap, 5);
		
		if (logPower)
			return spectrogram.getLogSpectraAmplitude();
		else
			return spectrogram.getSpectraAmplitude();
	}
	
	/**
	 * Computes the FFT of the <code>Wave</code>.  This function will
	 * perform the necessary zero-padding in order to make the samples buffer
	 * be an power of 2 in size.
	 *  
	 * @return the FFT (n rows x 2 column [real/imaginary]) array
	 * @see FFT
	 */
	// TODO: fix Wave.NO_DATA to work with doubles
	public double[][] fft()
	{
		int n = samples();
		int p2 = (int)Math.ceil(Math.log((double)n) / Math.log(2));
		int newSize = (int)Math.pow(2, p2);
		double[] buf = new double[newSize * 2];
		double m = mean();
		
		reset();
		double d;
		int i = 0;
		while (hasNext())
		{
			d = next();
			if (d == Wave.NO_DATA)
				buf[i * 2] = m;
			else
				buf[i * 2] = d;
			i++;
		}
		for (; i < newSize; i++)
		{
			buf[i * 2] = m;
		}
	
		FFT.fft(buf);
		
		double[][] r = new double[newSize][2];
		for (i = 0; i < newSize; i++)
		{
			r[i][0] = buf[i * 2];
			r[i][1] = buf[i * 2 + 1];
		}
		return r;
	}
	
	/**
	 * The same as FFT(), without separation real and imaginary part
	 *  
	 * @return the FFT (n*2 rows) array
	 * @see FFT
	 */
	public double[] fastFFT()
	{
		int n = samples();
		int p2 = (int)Math.floor(Math.log((double)n) / Math.log(2));
		int newSize = (int)Math.pow(2, p2);
		double[] buf = new double[newSize * 2];
		double m = mean();
		reset();
		
		for (int i = 0; i < newSize; i++) {
			if (hasNext()) {
				buf[i * 2] = next();
				if (buf[i * 2] == Wave.NO_DATA)
					buf[i * 2] = m;
			}
			else
				buf[i * 2] = 0;
		}

		FFT.fft(buf);
		
		return buf;
	}
	
	/**
	 * Set read pointer in the slice starting position
	 */
	public void reset()
	{
		readPosition = position;
	}
	
	/**
	 * Check if read pointer in the slice zone
	 * @return true if more to read
	 */
	public boolean hasNext()
	{
		return readPosition < limit;
	}
	
	/**
	 * Move read pointer in the next position
	 * @return data value in the read pointer position before moving
	 */
	public double next()
	{
		return source.buffer[readPosition++];
	}
	
	/**
	 * Dump slice content to CSV string
	 * @return CSV string
	 */
	public String toCSV()
	{
		if (source.buffer == null || source.buffer.length == 0)
		{
			return "";	
		}
		
		StringBuffer sb = new StringBuffer();
		double time = getStartTime();
		
		for (int i = position; i < limit; i++)
		{
			sb.append(Util.j2KToDateString(time) + ',' + source.buffer[i] + '\n');
			time += (1 / getSamplingRate());
		}
		
		return sb.toString();
	}
	
}
