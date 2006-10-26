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
	
	protected FrameDecorator decorator;
	
	public SpectraRenderer()
	{}

	public void setFrameDecorator(FrameDecorator fd)
	{
		decorator = fd;
	}
	
	public void setWave(SliceWave sw)
	{
		wave = sw;
	}
	
	public void setTitle(String t)
	{
		channelTitle = t;
	}
	
	protected class DefaultSpectraFrameDecorator extends DefaultFrameDecorator
	{
		public DefaultSpectraFrameDecorator()
		{
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
			
			yAxisLabel = "Power";
		}
	}
	

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

		createDefaultLineRenderers();
		decorator.decorate(this);
		
		return maxp;
	}

	public boolean isAutoScale()
	{
		return autoScale;
	}

	public void setAutoScale(boolean autoScale)
	{
		this.autoScale = autoScale;
	}
	
	public boolean isLogFreq()
	{
		return logFreq;
	}

	public void setLogFreq(boolean logFreq)
	{
		this.logFreq = logFreq;
	}

	public boolean isLogPower()
	{
		return logPower;
	}

	public void setLogPower(boolean logPower)
	{
		this.logPower = logPower;
	}

	public double getMaxFreq()
	{
		return maxFreq;
	}

	public void setMaxFreq(double maxFreq)
	{
		this.maxFreq = maxFreq;
	}

	public double getMaxPower()
	{
		return maxPower;
	}

	public void setMaxPower(double maxPower)
	{
		this.maxPower = maxPower;
	}

	public double getMinFreq()
	{
		return minFreq;
	}

	public void setMinFreq(double minFreq)
	{
		this.minFreq = minFreq;
	}
}
