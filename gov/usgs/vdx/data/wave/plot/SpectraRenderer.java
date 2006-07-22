package gov.usgs.vdx.data.wave.plot;

import gov.usgs.math.FFT;
import gov.usgs.plot.FrameDecorator;
import gov.usgs.plot.FrameRenderer;
import gov.usgs.plot.MatrixRenderer;
import gov.usgs.vdx.data.wave.SliceWave;
import cern.colt.matrix.DoubleFactory2D;
import cern.colt.matrix.DoubleMatrix2D;

/**
 * 
 * TODO: different axis labeling schemes.
 * $Log: not supported by cvs2svn $
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
	
	protected class DefaultFrameDecorator implements FrameDecorator
	{
		public void decorate(FrameRenderer fr)
		{
			int hTicks = graphWidth / 92;
			int vTicks = graphHeight / 24;
			createDefaultAxis(hTicks, vTicks, false, false);
			if (logFreq)
				createDefaultLogXAxis(5);	
			if (logPower)
				createDefaultLogYAxis(5);
				
			createDefaultLineRenderers();
			getAxis().setLeftLabelAsText("Power", -52);
			getAxis().setBottomLeftLabelAsText("Frequency");
//			TextRenderer tr = (TextRenderer)getAxis().getBottomLeftRenderer();
//			tr.y -= 16;
//			tr.x -= 4;
		}
	}
	
	public double update(double oldMaxPower)
	{
		if (decorator == null)
			decorator = new DefaultFrameDecorator();

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
