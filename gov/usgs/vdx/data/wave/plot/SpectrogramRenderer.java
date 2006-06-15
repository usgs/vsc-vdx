package gov.usgs.vdx.data.wave.plot;

import gov.usgs.plot.AxisRenderer;
import gov.usgs.plot.ImageDataRenderer;
import gov.usgs.plot.Jet;
import gov.usgs.plot.Spectrum;
import gov.usgs.vdx.data.wave.SliceWave;

import java.awt.Image;
import java.awt.Toolkit;
import java.awt.image.MemoryImageSource;

/**
 * $Log: not supported by cvs2svn $
 * Revision 1.3  2006/01/27 22:18:06  tparker
 * Add configure options for wave plotter
 *
 * Revision 1.2  2005/09/03 18:55:14  dcervelli
 * Change for log power rendering.
 *
 * Revision 1.1  2005/08/26 20:39:00  dcervelli
 * Initial avosouth commit.
 *
 * @author Dan Cervelli
 */
public class SpectrogramRenderer extends ImageDataRenderer
{
	private static final int[] SAMPLE_SIZES = new int[] {64, 128, 256, 512, 1024, 2048, 4096, 8192, 16384};
	private String fftSize;

	protected int hTicks;
	protected int vTicks;
	
	protected boolean logPower;
	protected boolean logFreq;
	protected boolean autoScale;
	protected double minFreq;
	protected double maxFreq;
	protected double maxPower;
	protected double viewStartTime;
	protected double viewEndTime;
	protected double overlap;
	protected double xLabel;
	protected double yLabel;
	
	protected byte[] imgBuffer;
	protected Spectrum spectrum;
	protected AxisRenderer axis;
	protected MemoryImageSource mis;
	protected Image im;
	
	protected SliceWave wave;
	
	protected double timeZoneOffset;
	
	public SpectrogramRenderer()
	{
		axis = new AxisRenderer(this);
		hTicks = -1;
		vTicks = -1;
		minFreq = 0.75;
		maxFreq = 20;
		maxPower = -Double.MAX_VALUE;
		overlap = 0.2;
		fftSize = "Auto";
		autoScale = true;
		logPower = false;
		logFreq = false;
		imgBuffer = new byte[64000];
		spectrum = Jet.getInstance();
	}
	  
	public SpectrogramRenderer(SliceWave w)
	{
		this();
		wave = w;
	}

	public double update(double oldMaxPower)
	{
		wave.setSlice(viewStartTime, viewEndTime);
		
		int sampleSize = 128;
			
		if (fftSize.toLowerCase().equals("auto"))
		{
			double bestFit = 1E300;
			int bestIndex = -1;
			for (int i = 0; i < SAMPLE_SIZES.length; i++)
			{
				double xs = (double)wave.samples() / (double)SAMPLE_SIZES[i];
				double ys = (double)SAMPLE_SIZES[i] / 2;
				double ar = xs / ys;
				double fit = Math.abs(ar - 1);
				if (fit < bestFit)
				{
					bestFit = fit;
					bestIndex = i;
				}
			}
			sampleSize = SAMPLE_SIZES[bestIndex];
		}
		else
			sampleSize = Integer.parseInt(fftSize);

		int imgXSize = wave.samples() / sampleSize;
//		int imgYSize = sampleSize / 2;
		int imgYSize = sampleSize / 4;
		if (imgXSize <= 0 || imgYSize <= 0)
			return -1;

		double minF = minFreq;
		double maxF = maxFreq;
		double maxMag = -1E300;
		double mag, f;
		double[][] powerBuffer = wave.toSpectrogram(sampleSize, logPower, logFreq, overlap);
		imgYSize = powerBuffer[0].length;
		imgXSize = powerBuffer.length;
		
		for (int i = 0; i < imgXSize; i++)
		{
			for (int j = 0; j < imgYSize; j++)
			{
				f = ((double)j / (double)(imgYSize)) * wave.getSamplingRate() / 2;
				mag = powerBuffer[i][j];
				if (f >= minF && f <= maxF && mag > maxMag)
					maxMag = mag;
			}
		}
		
		if (autoScale)		
		{
			if (logPower)
				maxMag = Math.pow(10, maxMag);
			
			maxMag = Math.max(maxMag, oldMaxPower);
		}
		else
			maxMag = maxPower;

		if (logPower)
			maxMag = Math.log(maxMag) / Math.log(10);
		
		if (imgBuffer.length < imgXSize * imgYSize)
			imgBuffer = new byte[imgXSize * imgYSize];
		
		double logMinMag = maxMag - 3;
		for (int i = 0; i < imgXSize; i++)
			for (int j = imgYSize - 1, k = 0; j >= 0; j--, k++)
			{
				double ratio = logPower ? (powerBuffer[i][j] - logMinMag) / (maxMag - logMinMag) : powerBuffer[i][j] / maxMag;
				if (ratio < 0)
					ratio = 0;
				if (ratio > 1)
					ratio = 1;
				imgBuffer[i + imgXSize * k] = (byte)(spectrum.getColorIndexByRatio(ratio) + 9);
			}

		if (mis == null || im.getWidth(null) != imgXSize || im.getHeight(null) != imgYSize)
		{
			mis = new MemoryImageSource(imgXSize, imgYSize, spectrum.palette,
					imgBuffer, 0, imgXSize);
		}
		
		im = Toolkit.getDefaultToolkit().createImage(mis);
		
		this.setImage(im);
		//this.setDataExtents(viewStartTime + timeZoneOffset, viewEndTime + timeZoneOffset, 0, wave.getSamplingRate() / 2);				 
		this.setDataExtents(wave.getStartTime() + timeZoneOffset, wave.getEndTime() + timeZoneOffset, 0, wave.getSamplingRate() / 2);
		this.setExtents(viewStartTime + timeZoneOffset, viewEndTime + timeZoneOffset, minF, maxF);
		int ht = hTicks;
		int vt = vTicks;
		if (hTicks == -1)
			ht = graphWidth / 108;
		if (vTicks == -1)
			vt = graphHeight / 24;
		
		if (xLabel == 0 && yLabel == 2)
		{
			this.createDefaultAxis(0, 0, false, false);
			this.getAxis().createDefault();			
		} else {			
			this.createDefaultAxis(ht, vt, false, false);
			this.getAxis().createDefault();
			this.setXAxisToTime(ht);
			this.getAxis().setLeftLabelAsText("Frequency (Hz)", -52);
			this.getAxis().setBottomLeftLabelAsText("Time");
		}
		return maxMag;
	}

	/**
	 * @param autoScale The autoScale to set.
	 */
	public void setAutoScale(boolean autoScale)
	{
		this.autoScale = autoScale;
	}
	/**
	 * @param fftSize The fftSize to set.
	 */
	public void setFftSize(String fftSize)
	{
		this.fftSize = fftSize;
	}
	/**
	 * @param logFreq The logFreq to set.
	 */
	public void setLogFreq(boolean logFreq)
	{
		this.logFreq = logFreq;
	}
	/**
	 * @param logPower The logPower to set.
	 */
	public void setLogPower(boolean logPower)
	{
		this.logPower = logPower;
	}
	/**
	 * @param maxFreq The maxFreq to set.
	 */
	public void setMaxFreq(double maxFreq)
	{
		this.maxFreq = maxFreq;
	}
	/**
	 * @param maxPower The maxPower to set.
	 */
	public void setMaxPower(double maxPower)
	{
		this.maxPower = maxPower;
	}
	/**
	 * @param minFreq The minFreq to set.
	 */
	public void setMinFreq(double minFreq)
	{
		this.minFreq = minFreq;
	}
	/**
	 * @param spectrogramOverlap The spectrogramOverlap to set.
	 */
	public void setOverlap(double overlap)
	{
		this.overlap = overlap;
	}
	/**
	 * @param viewEndTime The viewEndTime to set.
	 */
	public void setViewEndTime(double viewEndTime)
	{
		this.viewEndTime = viewEndTime;
	}
	/**
	 * @param viewStartTime The viewStartTime to set.
	 */
	public void setViewStartTime(double viewStartTime)
	{
		this.viewStartTime = viewStartTime;
	}
	/**
	 * @param wave The wave to set.
	 */
	public void setWave(SliceWave wave)
	{
		this.wave = wave;
	}
	
	/**
	 * @param ticks The hTicks to set.
	 */
	public void setHTicks(int ticks)
	{
		hTicks = ticks;
	}
	/**
	 * @param ticks The vTicks to set.
	 */
	public void setVTicks(int ticks)
	{
		vTicks = ticks;
	}
	
	public void setTimeZoneOffset(double tzo)
	{
		timeZoneOffset = tzo;
	}
	
	public void setYLabel(int i)
	{
		yLabel = i;
	}

	public void setXLabel(int i)
	{
		xLabel = i;
	}
}
