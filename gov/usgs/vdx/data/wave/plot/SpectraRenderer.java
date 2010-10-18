package gov.usgs.vdx.data.wave.plot;

import gov.usgs.math.FFT;
import gov.usgs.plot.DefaultFrameDecorator;
import gov.usgs.plot.FrameDecorator;
import gov.usgs.plot.MatrixRenderer;
import gov.usgs.vdx.data.wave.SliceWave;

import java.awt.Color;

import cern.colt.matrix.DoubleFactory2D;
import cern.colt.matrix.DoubleMatrix2D;

/**
 * Renderer for spectra data
 * 
 * TODO: different axis labeling schemes.
 * $Log: not supported by cvs2svn $
 * Revision 1.3  2006/07/25 16:23:33  cervelli
 * Changes for new DefaultFrameDecorator.
 *
 * Revision 1.2  2006/07/22 20:15:45  cervelli
 * Interim changes for conversion to FrameDecorators.
 *
 * Revision 1.1  2005/09/04 18:13:34  dcervelli
 * Initial commit.
 *
 * @author Dan Cervelli
 */
public class SpectraRenderer extends MatrixRenderer
{
	private SliceWave wave;
	
	private double minFreq;
	private double maxFreq;
	private boolean logFreq;
	private boolean logPower;
	private double maxPower;
	private boolean autoScale;
	private String channelTitle;
	
	public boolean xTickMarks = true;
	public boolean xTickValues = true;
	public boolean xUnits = true;
	public boolean xLabel = false;
	public boolean yTickMarks = true;
	public boolean yTickValues = true;
	private String yLabelText = null;
	private String yUnitText = null;
	private Color color = null;
	protected String timeZone;
	
	protected FrameDecorator decorator;
	
	/**
	 * Default constructor
	 */
	public SpectraRenderer()
	{}
	
	/**
	 * Set frame decorator to draw graph's frame
	 * @param  fd frame decorator
	 */
	public void setFrameDecorator(FrameDecorator fd)
	{
		decorator = fd;
	}
	
	/**
	 * Set slice to render
	 * @param sw slice to render
	 */
	public void setWave(SliceWave sw)
	{
		wave = sw;
	}
	
	/**
	 * Set graph title
	 * @param t title
	 */
	public void setTitle(String t)
	{
		channelTitle = t;
	}
	
	protected class DefaultSpectraFrameDecorator extends DefaultFrameDecorator
	{
		public DefaultSpectraFrameDecorator()
		{
			if(yUnitText != null){
				this.yUnit = yUnitText;
			}
			if(yLabelText != null){
				this.yAxisLabel = yLabelText;
			}
			if(xUnits){
				this.xUnit = "Frequency (Hz)";
			}
			this.xAxisLabels = xTickValues;
			this.yAxisLabels = yTickValues;
			if(!xTickMarks){
				hTicks=0;
				xAxisGrid = Grid.NONE;
			}
			if(!yTickMarks){
				vTicks=0;
				yAxisGrid = Grid.NONE;
			}
			this.title = channelTitle;
			this.titleBackground = Color.WHITE;
		}
		
		public void update()
		{
			if (logFreq)
				xAxis = DefaultFrameDecorator.XAxis.LOG;
			else
				xAxis = DefaultFrameDecorator.XAxis.LINEAR;
			
			if (logPower)
				yAxis = DefaultFrameDecorator.YAxis.LOG;
			else
				yAxis = DefaultFrameDecorator.YAxis.LINEAR;
		}
	}
	
	/**
	 * Compute spectra for slice.
	 * Reinitialize frame decorator with this renderer data.
	 * @param oldMaxPower 
	 * @return maximum spectra power value
	 */
	public double update(double oldMaxPower)
	{
		if (decorator == null)
			decorator = new DefaultSpectraFrameDecorator();

		decorator.update();
		double[] data = wave.fastFFT();
		FFT.fastToPowerFreq(data, wave.getSamplingRate(), logPower, logFreq);
		if (logFreq)
		{
			if (minFreq == 0)
				minFreq = data[3 * 2];
			else
				minFreq = Math.log(minFreq) / FFT.LOG10;
			maxFreq = Math.log(maxFreq) / FFT.LOG10;
		}
		double maxp = -1E300;
		double minp = 1E300;
		int n = data.length / 4;
		for (int i = 2; i < n; i++)
		{
			if (data[i * 2] >= minFreq && data[i * 2] <= maxFreq)
			{
				if (data[i * 2 + 1] > maxp)
					maxp = data[i * 2 + 1];
				if (data[i * 2 + 1] < minp)
					minp = data[i * 2 + 1];
			}
		}
		
		DoubleMatrix2D dm = DoubleFactory2D.dense.make(n, 2);
		for (int i = 0; i < n; i++)
		{
			dm.setQuick(i, 0, data[i * 2]);
			dm.setQuick(i, 1, data[i * 2 + 1]);
		}
		setData(dm);

		if (autoScale)		
		{
			if (logPower)
				maxp = Math.pow(10, maxp);
			
			maxp = Math.max(maxp, oldMaxPower);
		}
		else
			maxp = maxPower;
		
		if (logPower)
			maxp = Math.log(maxp) / Math.log(10);
		setExtents(minFreq, maxFreq, 0, maxp);
		
//		createDefaultAxis(hTicks, vTicks, false, false);
//		if (logFreq)
//			createDefaultLogXAxis(5);	
//		if (logPower)
//			createDefaultLogYAxis(5);
//			
//		createDefaultLineRenderers();
//		getAxis().setLeftLabelAsText("Power", -52);
//		getAxis().setBottomLeftLabelAsText("Freq.");

		createDefaultLineRenderers(color);
		decorator.decorate(this);
		
		return maxp;
	}

	/**
	 * Get autoscale flag
	 * @return autoscale flag
	 */
	public boolean isAutoScale()
	{
		return autoScale;
	}

	/**
	 * Set autoscale flag
	 * @param autoScale flag
	 */
	public void setAutoScale(boolean autoScale)
	{
		this.autoScale = autoScale;
	}

	/**
	 * Get flag if we have logarithm frequency axis
	 * @return log freq flag
	 */
	public boolean isLogFreq()
	{
		return logFreq;
	}

	/**
	 * Set flag if we have logarithm frequency axis
	 * @param logFreq true if freq axis is logarithmic
	 */
	public void setLogFreq(boolean logFreq)
	{
		this.logFreq = logFreq;
	}

	/**
	 * Get flag if we have logarithm power axis
	 * @return log power flag
	 */
	public boolean isLogPower()
	{
		return logPower;
	}

	/**
	 * Set flag if we have logarithm power axis
	 * @param logPower true if power axis is logarithmic
	 */
	public void setLogPower(boolean logPower)
	{
		this.logPower = logPower;
	}

	/**
	 * Get maximum frequency
	 * @return maximum frequency
	 */
	public double getMaxFreq()
	{
		return maxFreq;
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
	 * Get maximum power
	 * @return maximum power
	 */
	public double getMaxPower()
	{
		return maxPower;
	}

	/**
	 * Set maximum power
	 * @param maxPower maximum power
	 */
	public void setMaxPower(double maxPower)
	{
		this.maxPower = maxPower;
	}

	/**
	* Get minimum frequency
	* @return minimum frequency
	public double getMinFreq()
	{
		return minFreq;
	}

	/**
	 * Set minimum frequency
	 * @param minimum frequency
	 */
	public void setMinFreq(double minFreq)
	{
		this.minFreq = minFreq;
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
	 * Set color
	 * @param color color
	 */
	public void setColor(Color color)
	{
		this.color = color;
	}
}
