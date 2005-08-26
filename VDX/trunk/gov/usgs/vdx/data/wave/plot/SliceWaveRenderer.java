package gov.usgs.vdx.data.wave.plot;

import gov.usgs.plot.FrameRenderer;
import gov.usgs.vdx.data.wave.SliceWave;
import gov.usgs.vdx.data.wave.Wave;

import java.awt.Color;
import java.awt.Graphics2D;
import java.awt.Rectangle;
import java.awt.Shape;
import java.awt.geom.GeneralPath;

/**
 * A renderer for wave time series.
 * 
 * $Log: not supported by cvs2svn $
 * Revision 1.3  2005/03/25 00:17:19  cervelli
 * Comment update.
 *
 * Revision 1.2  2005/03/25 00:15:36  cervelli
 * Fixed bug where NO_DATA gaps weren't being drawn properly.
 *
 * @author Dan Cervelli
 */
public class SliceWaveRenderer extends FrameRenderer
{
//	private Rectangle rectangle = new Rectangle();
//	private Line2D.Double line = new Line2D.Double();
	private GeneralPath gp;
	private SliceWave wave;
	
	protected boolean autoScale = true;
	protected boolean removeBias = true;
	
	protected double highlightX1;
	protected double highlightX2;
	protected double viewStartTime;
	protected double viewEndTime; 
	
	public void setHighlight(double x1, double x2)
	{
		highlightX1 = x1;
		highlightX2 = x2;
	}
	
	public double getMaxY() 
	{
		return maxY;
	}
	
	public void setMaxY(double maxY) 
	{
		this.maxY = maxY;
	}
	
	public double getMinY() 
	{
		return minY;
	}
	
	public void setMinY(double minY) 
	{
		this.minY = minY;
	}
	
	public boolean isAutoScale() 
	{
		return autoScale;
	}
	
	public boolean isRemoveBias() 
	{
		return removeBias;
	}
	
	public void setAutoScale(boolean b)
	{
		autoScale = b;
	}
	
	public void setYLimits(double min, double max)
	{
		minY = min;
		maxY = max;
	}
	
	public void setRemoveBias(boolean b)
	{
	    removeBias = b;
	}
	
	public void setWave(SliceWave w)
	{
		wave = w;
	}

	public void setViewTimes(double t1, double t2)
	{
	    viewStartTime = t1;
	    viewEndTime = t2;
	}
	
	public void update()
	{
		this.setExtents(viewStartTime, viewEndTime, minY, maxY);
		int hTicks = graphWidth / 108;
		int vTicks = graphHeight / 24;
		this.createDefaultAxis(hTicks, vTicks);
		this.getAxis().createDefault();
		this.setXAxisToTime(hTicks);
		this.getAxis().setLeftLabelAsText("Counts", -52);
		this.getAxis().setBottomLeftLabelAsText("Time");
	}
	
	public void render(Graphics2D g)
	{
		Shape origClip = g.getClip();
		
		if (axis != null)
			axis.render(g);
		
        g.clip(new Rectangle(graphX + 1, graphY + 1, graphWidth - 1, graphHeight - 1));
		
		double st = wave.getStartTime();
		double step = 1 / wave.getSamplingRate();
		
		if (gp == null)
			gp = new GeneralPath();
		
		gp.reset();
		wave.reset();
		int y = wave.next();
		
		double bias = 0;
		if (removeBias)
		    bias = wave.mean();
		
		gp.moveTo((float)getXPixel(st), (float)(getYPixel(y - bias)));
		
		g.setColor(Color.blue);

		float lastY = (float)getYPixel(y - bias);
		while (wave.hasNext())
		{
			st += step;
			y = wave.next();
			if (y == Wave.NO_DATA)
				gp.moveTo((float)getXPixel(st), lastY);
			else
			{
				lastY = (float)getYPixel(y - bias);
				gp.lineTo((float)getXPixel(st), lastY);
			}
		}
//			g.setRenderingHint(RenderingHints.KEY_ANTIALIASING, RenderingHints.VALUE_ANTIALIAS_ON);
		g.draw(gp);
//			g.setRenderingHint(RenderingHints.KEY_ANTIALIASING, RenderingHints.VALUE_ANTIALIAS_OFF);

		g.setClip(origClip);
	}
}
