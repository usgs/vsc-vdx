package gov.usgs.vdx.data.wave.plot;

import gov.usgs.plot.DefaultFrameDecorator;
import gov.usgs.plot.FrameDecorator;
import gov.usgs.plot.MatrixRenderer;
import gov.usgs.vdx.data.wave.SliceWave;

import java.awt.Color;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

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

	private String channelTitle;
	
	private boolean logFreq;
	private boolean logPower;
	private boolean autoScale;
	
	public boolean xTickMarks = true;
	public boolean xTickValues = true;
	public boolean xUnits = true;
	public boolean xLabel = true;
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
		channelTitle = t.split("\\.")[0];
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
			xAxis = (logFreq) ? DefaultFrameDecorator.XAxis.LOG: DefaultFrameDecorator.XAxis.LINEAR;
			yAxis = (logPower) ? DefaultFrameDecorator.YAxis.LOG: DefaultFrameDecorator.YAxis.LINEAR;
		}
		
	}
	
	/**
	 * Compute spectra for slice.
	 * Reinitialize frame decorator with this renderer data.
	 * @param oldMaxPower 
	 * @return maximum spectra power value
	 */
	
	public void update()
	{
		DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss.SSS (z)");
		Date date = new Date();
		System.out.printf("Updating spectra at: %s\n", dateFormat.format(date));
		
		if (decorator == null)
			decorator = new DefaultSpectraFrameDecorator();

		decorator.update();
		
		double[] data = wave.fastFFT(); // FFT of the wave: [real;imag;real;imag ...]
		double delta = wave.getSamplingRate() / data.length * 2;
		double P, F;
		double maxp = -1E300;
		double minp = 1E300;

		DoubleMatrix2D dm = DoubleFactory2D.dense.make(data.length / 4, 2);

		for (int i = 2; i <= data.length / 2; i=i+2) {
			
			F = i/2 *delta;
			P = Math.sqrt(data[i]*data[i]+data[i+1]*data[i+1]);
			
			if (logFreq)
				F = Math.log10(F);
			
			if (logPower)
				P = Math.log10(P);

			dm.setQuick(i/2 - 1, 0, F);
			dm.setQuick(i/2 - 1, 1, P);
			
			if (P > maxp)
				maxp = P;
			if (P < minp)
				minp = P;
			
//			if (F >= minFreq && F <= maxFreq) {
//		
//			}
		}
		
		double minf = delta;
		double maxf = data.length / 4 * delta;
		
		setData(dm);
		
		setVisible(0, false);

		double X1, X2, Y1, Y2;
		
		if (logPower) {
			Y1 = Math.floor(minp);
			Y2 = Math.ceil(maxp);
		}
		else {
			Y1 = minp;
			Y2 = maxp;
		}
			
		if (logFreq) {
			X1 = Math.log10(Math.max(minFreq, minf));
			X2 = Math.log10(Math.min(maxFreq, maxf));
		}
		else {
			X1 = Math.max(minFreq, minf);
			X2 = Math.min(maxFreq, maxf);
		}
		
		setExtents(X1, X2, Y1, Y2);
		
		createDefaultLineRenderers(color);
		decorator.decorate(this);
		
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
	* Get minimum frequency
	* @return minimum frequency
	*/
	public double getMinFreq()
	{
		return minFreq;
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
