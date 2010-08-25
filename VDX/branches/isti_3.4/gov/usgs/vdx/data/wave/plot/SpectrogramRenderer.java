package gov.usgs.vdx.data.wave.plot;

import gov.usgs.plot.AxisRenderer;
import gov.usgs.plot.DefaultFrameDecorator;
import gov.usgs.plot.FrameDecorator;
import gov.usgs.plot.ImageDataRenderer;
import gov.usgs.plot.Jet;
import gov.usgs.plot.Spectrum;
import gov.usgs.util.Util;
import gov.usgs.vdx.data.wave.SliceWave;

import java.awt.Color;
import java.awt.Image;
import java.awt.Toolkit;
import java.awt.image.MemoryImageSource;

/**
 * Renderer to draw spectrograms.
 * Keeps reference to processed wave data,
 * compute spectrograms and create image to render as ImageDataRenderer.
 *   
 * $Log: not supported by cvs2svn $
 * Revision 1.6  2006/10/26 01:04:44  dcervelli
 * Changes for labeling.
 *
 * Revision 1.5  2006/08/06 20:02:31  cervelli
 * Switched to decorators.
 *
 * Revision 1.4  2006/06/15 14:29:56  dcervelli
 * Swarm 1.3.4 changes.
 *
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
		
	public boolean xTickMarks = true;
	public boolean xTickValues = true;
	public boolean xUnits = true;
	public boolean xLabel = false;
	public boolean yTickMarks = true;
	public boolean yTickValues = true;
    protected String timeZone;
	private String yLabelText = null;
	private String yUnitText = null;
	protected byte[] imgBuffer;
	protected Spectrum spectrum;
	protected AxisRenderer axis;
	protected MemoryImageSource mis;
	protected Image im;
	
	protected SliceWave wave;
	
	protected FrameDecorator decorator;
	
	protected String channelTitle;
	
	/**
	 * Default constructor
	 */
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
	
	/**
	 * Constructor
	 * @param w slice to present as spectrogram
	 */
	public SpectrogramRenderer(SliceWave w)
	{
		this();
		wave = w;
	}

	public void setFrameDecorator(FrameDecorator fd)
	{
		decorator = fd;
	}
	
	/**
	 * Create default decorator to render frame
	 */
	public void createDefaultFrameDecorator()
	{
		decorator = new DefaultWaveFrameDecorator();
	}
	
	/**
	 * Set graph title
	 */
	public void setTitle(String t)
	{
		channelTitle = t;
	}
	
	protected class DefaultWaveFrameDecorator extends DefaultFrameDecorator
	{
		public DefaultWaveFrameDecorator()
		{
			if(yUnitText != null){
				this.yUnit = yUnitText;
			}
			if(yLabelText != null){
				this.yAxisLabel = yLabelText;
			}
			if(xUnits){
				this.xUnit = timeZone + " Time (" + Util.j2KToDateString(viewStartTime, "yyyy MM dd") + " to " + Util.j2KToDateString(viewEndTime, "yyyy MM dd")+ ")";
			}
			this.xAxisLabels = xTickValues;
			this.yAxisLabels = yTickValues;
			if(!xTickMarks){
				vTicks=0;
			}
			if(!yTickMarks){
				hTicks=0;
			}
			this.title = channelTitle;
			this.titleBackground = Color.WHITE;
		}
	}
	
	/**
	 * Compute spectrogram.
	 * Reinitialize frame decorator with this renderer data.
	 * @return maximum magnitude
	 */
	public double update(double oldMaxPower)
	{
		if (decorator == null)
			createDefaultFrameDecorator();
		
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
		this.setDataExtents(wave.getStartTime(), wave.getEndTime(), 0, wave.getSamplingRate() / 2);
		this.setExtents(viewStartTime, viewEndTime, minF, maxF);
		decorator.decorate(this);

		return maxMag;
	}

	/**
	 * Set autoscale flag
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
	 * Set flag if we have logarithm frequency axis
	 */
	public void setLogFreq(boolean logFreq)
	{
		this.logFreq = logFreq;
	}
	
	/**
	 * Set flag if we have logarithm power axis
	 */
	public void setLogPower(boolean logPower)
	{
		this.logPower = logPower;
	}
	
	/**
	 * Set maximum frequency
	 */
	public void setMaxFreq(double maxFreq)
	{
		this.maxFreq = maxFreq;
	}
	
	/**
	 * Get maximum power value
	 */
	public void setMaxPower(double maxPower)
	{
		this.maxPower = maxPower;
	}
	
	/**
	 * Set minimum frequency
	 */
	public void setMinFreq(double minFreq)
	{
		this.minFreq = minFreq;
	}
	/**
	 * Set spectrogram overlapping flag
	 */
	public void setOverlap(double overlap)
	{
		this.overlap = overlap;
	}
	/**
	 * Set viewEndTime.
	 */
	public void setViewEndTime(double viewEndTime)
	{
		this.viewEndTime = viewEndTime;
	}
	
	/**
	 * Set viewStartTime.
	 */
	public void setViewStartTime(double viewStartTime)
	{
		this.viewStartTime = viewStartTime;
	}
	
	/**
	 * Set Time Zone name.
	 */
	public void setTimeZone(String timeZone)
	{
		this.timeZone = timeZone;
	}
	
	/**
	 * Set Y axis label
	 */
	public void setYLabelText(String s)
	{
		yLabelText = s;
	}
	
	/**
	 * Set Y axis unit
	 */
	public void setYUnitText(String s)
	{
		yUnitText = s;
	}
	
	/**
	 * Set slice to process
	 */
	public void setWave(SliceWave wave)
	{
		this.wave = wave;
	}
	
	/**
	 * Set h ticks count
	 */
	public void setHTicks(int ticks)
	{
		hTicks = ticks;
	}
	
	/**
	 * Set v ticks count
	 */
	public void setVTicks(int ticks)
	{
		vTicks = ticks;
	}
}
