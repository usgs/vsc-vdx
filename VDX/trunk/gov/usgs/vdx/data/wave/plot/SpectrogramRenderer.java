package gov.usgs.vdx.data.wave.plot;

import gov.usgs.plot.AxisRenderer;
import gov.usgs.plot.DefaultFrameDecorator;
import gov.usgs.plot.FrameDecorator;
import gov.usgs.plot.ImageDataRenderer;
import gov.usgs.plot.Jet2;
import gov.usgs.plot.Spectrum;
import gov.usgs.util.Util;
import gov.usgs.vdx.data.wave.SliceWave;

import java.awt.Color;
import java.awt.Image;
import java.awt.Toolkit;
import java.awt.image.MemoryImageSource;
// import java.io.BufferedReader;
// import java.io.FileNotFoundException;
// import java.io.FileReader;
// import java.util.Scanner;

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

	protected int hTicks;
	protected int vTicks;
	protected int nfft;
	protected int binSize;
	
	protected boolean logPower;
	protected boolean logFreq;
	protected boolean autoScale;
	
	protected double minFreq;
	protected double maxFreq;
	// protected double minScale;
	// protected double maxScale;
	protected double minPower;
	protected double maxPower;
	protected double overlap;
	protected double viewStartTime;
	protected double viewEndTime;
		
	public boolean xTickMarks = true;
	public boolean xTickValues = true;
	public boolean xUnits = true;
	public boolean xLabel = false;
	public boolean yTickMarks = true;
	public boolean yTickValues = true;
    protected String timeZone;
	protected String dateFormatString = "yyyy-MM-dd HH:mm:ss";
	
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
		// minScale = 20;
		// maxScale = 120;
		// maxPower = -Double.MAX_VALUE;
		minPower = 20;
		maxPower = 120;
		overlap = 0.859375;
		nfft = 0; // Auto
		binSize = 256;
		autoScale = false;
		logPower = false;
		logFreq = false;
		spectrum = Jet2.getInstance();
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

	/**
	 * Set frame decorator
	 * @param fd frame decorator
	 */
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
	 * @param t title
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
				this.xUnit = timeZone + " Time (" + Util.j2KToDateString(viewStartTime, dateFormatString) + " to " + Util.j2KToDateString(viewEndTime, dateFormatString)+ ")";
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
	 * @param oldMaxPower 
	 * @return maximum magnitude
	 */
	public double update(double oldMaxPower)
	{
		/*
        Scanner s;
		try {
			s = new Scanner(new BufferedReader(new FileReader("d:/sgram.config")));
	        nfft = s.nextInt();
	        binSize = s.nextInt();
	        overlap = s.nextDouble();
	        minPower = s.nextDouble();
	        maxPower = s.nextDouble();
	        // minScale = s.nextDouble();
	        // maxScale = s.nextDouble();
	        s.close();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		*/

		if (decorator == null)
			createDefaultFrameDecorator();

		wave.setSlice(viewStartTime, viewEndTime);
		System.out.printf("%f %f\n", wave.getStartTime(),wave.getEndTime());
		
		// Overlap is expressed as percentage of bin size -- needs to be converted to absolute number of samples
		
		double[][] powerBuffer = wave.toSpectrogram(binSize, nfft, logPower, (int)(binSize * overlap));
		
		int imgXSize = powerBuffer.length;
		int imgYSize = powerBuffer[0].length; 
		
		System.out.printf("X: %d, Y: %d\n",imgXSize,imgYSize);
		
		imgBuffer = new byte[imgXSize * imgYSize];
		
		// Maps the range of power values to [0 254] (255/-1 is transparent). 

		if (autoScale) {
			maxPower = Double.MIN_VALUE;
			minPower = Double.MIN_VALUE;
			for (int i = 0; i < imgXSize; i++)
				for (int j = 0; j < imgYSize; j++) {
					if (powerBuffer[i][j] > maxPower)
						maxPower = powerBuffer[i][j];
					if (powerBuffer[i][j] < minPower)
						minPower = powerBuffer[i][j];
				}
			System.out.printf("Autoscaling from %f to %f\n",minPower,maxPower);
		}
				
		double slope = 254 / (maxPower - minPower);
		double intercept = -slope * minPower;
		int counter = 0;
		double index;
		for (int i = imgXSize -1; i >= 0; i--)
			for (int j = 0; j < imgYSize; j++) {
				index = slope * powerBuffer[i][j] + intercept;
				if (index<0)
					index=0;
				else if (index>254)
					index=254;
				imgBuffer[counter++] = (byte)index;
			}
	
//		FileWriter outFile;
//		try {
//		outFile = new FileWriter("d:/output.txt");
//		PrintWriter out = new PrintWriter(outFile);		
//
//		counter = 0;
//		for (int i = imgXSize -1; i >= 0; i--)
//			for (int j = 0; j < imgYSize; j++) {
//				out.printf("%f %d\n",powerBuffer[i][j],imgBuffer[counter++]);
//			}
//		out.close();
//		}
//		catch (IOException e) {
//			e.printStackTrace();
//		}

		
		if (mis == null || im.getWidth(null) != imgXSize || im.getHeight(null) != imgYSize)
		{
			mis = new MemoryImageSource(imgYSize, imgXSize, spectrum.palette,
					imgBuffer, 0, imgYSize);
		}
		
		im = Toolkit.getDefaultToolkit().createImage(mis);
		
		this.setImage(im);
		//this.setDataExtents(viewStartTime + timeZoneOffset, viewEndTime + timeZoneOffset, 0, wave.getSamplingRate() / 2);				 
		this.setDataExtents(wave.getStartTime(), wave.getEndTime(), 0, wave.getSamplingRate() / 2);
		this.setExtents(viewStartTime, viewEndTime, minFreq, maxFreq);
		decorator.decorate(this);

		return 15000.0;
	}

	/**
	 * Set autoscale flag
	 * @param autoScale autoscale flag
	 */
	public void setAutoScale(boolean autoScale)
	{
		this.autoScale = autoScale;
	}
	/**
	 * Set size of fft
	 * @param nfft Sets the number of points for the fft
	 */
	public void setNfft(int nfft)
	{
		this.nfft = nfft;
	}
	/**
	 * Set size of bin
	 * @param binSize The bin size to set.
	 */
	public void setBinSize(int binSize)
	{
		this.binSize = binSize;
	}
	/**
	 * Set flag if we have logarithm frequency axis
	 * @param logFreq logarithm frequency axis flag
	 */
	public void setLogFreq(boolean logFreq)
	{
		this.logFreq = logFreq;
	}
	
	/**
	 * Set flag if we have logarithm power axis
	 * @param logPower logarithm power axis flag
	 */
	public void setLogPower(boolean logPower)
	{
		this.logPower = logPower;
	}
	
	/**
	 * Set minimum power value
	 * @param minPower new minimum power
	 */
	public void setMinPower(double minPower)
	{
		this.minPower = minPower;
	}
	
	/**
	 * Set maximum power value
	 * @param maxPower new maximum power
	 */
	public void setMaxPower(double maxPower)
	{
		this.maxPower = maxPower;
	}
	
	/**
	 * Set minimum frequency
	 * @param minFreq minimum frequency
	 */
	public void setMinFreq(double minFreq)
	{
		this.minFreq = minFreq;
	}
	
	/**
	 * Set maximum frequency
	 * @param maxFreq maximum frequency
	 */
	public void setMaxFreq(double maxFreq)
	{
		this.maxFreq = maxFreq;
	}
	
	/**
	 * Set spectrogram overlapping flag
	 * @param overlap spectrogram overlapping flag
	 */
	public void setOverlap(double overlap)
	{
		this.overlap = overlap;
	}
	/**
	 * Set viewEndTime.
	 * @param viewEndTime view end time
	 */
	public void setViewEndTime(double viewEndTime)
	{
		this.viewEndTime = viewEndTime;
	}
	
	/**
	 * Set viewStartTime.
	 * @param viewStartTime view start time
	 */
	public void setViewStartTime(double viewStartTime)
	{
		this.viewStartTime = viewStartTime;
	}
	
	/**
	 * Set Time Zone name.
	 * @param timeZone time zone name
	 */
	public void setTimeZone(String timeZone)
	{
		this.timeZone = timeZone;
	}
	
	/**
	 * Set Y axis label
	 * @param s Y axis label
	 */
	public void setYLabelText(String s)
	{
		yLabelText = s;
	}
	
	/**
	 * Set Y axis unit
	 * @param s Y axis unit
	 */
	public void setYUnitText(String s)
	{
		yUnitText = s;
	}
	
	/**
	 * Set slice to process
	 * @param wave slice to process
	 */
	public void setWave(SliceWave wave)
	{
		this.wave = wave;
	}
	
	/**
	 * Set h ticks count
	 * @param ticks h ticks count
	 */
	public void setHTicks(int ticks)
	{
		hTicks = ticks;
	}
	
	/**
	 * Set v ticks count
	 * @param ticks v ticks count
	 */
	public void setVTicks(int ticks)
	{
		vTicks = ticks;
	}
}
