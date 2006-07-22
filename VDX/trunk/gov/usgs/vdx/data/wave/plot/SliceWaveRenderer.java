package gov.usgs.vdx.data.wave.plot;

import gov.usgs.plot.DefaultFrameDecorator;
import gov.usgs.plot.FrameDecorator;
import gov.usgs.plot.FrameRenderer;
import gov.usgs.vdx.data.wave.SliceWave;
import gov.usgs.vdx.data.wave.Wave;

import java.awt.Color;
import java.awt.Graphics2D;
import java.awt.Rectangle;
import java.awt.Shape;
import java.awt.geom.GeneralPath;
import java.awt.geom.Line2D;
import java.awt.geom.Rectangle2D;

/**
 * A renderer for wave time series.
 * 
 * $Log: not supported by cvs2svn $
 * Revision 1.8  2006/06/15 14:29:55  dcervelli
 * Swarm 1.3.4 changes.
 *
 * Revision 1.7  2006/03/02 23:35:31  dcervelli
 * Added calibration stuff.
 *
 * Revision 1.6  2006/02/19 00:32:11  dcervelli
 * Added option for drawing sample boxes.
 *
 * Revision 1.5  2006/01/27 20:57:27  tparker
 * Add configure options for wave plotter
 *
 * Revision 1.4  2005/09/03 20:31:16  dcervelli
 * Changed logic for deciding when to use optimized rendering method.
 *
 * Revision 1.3  2005/09/02 16:19:59  dcervelli
 * Major optimization for most waves.
 *
 * Revision 1.2  2005/09/01 00:30:02  dcervelli
 * Changes for SliceWave refactor.
 *
 * Revision 1.1  2005/08/26 20:39:00  dcervelli
 * Initial avosouth commit.
 *
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
	private SliceWave wave;
	
	protected boolean autoScale = true;
	protected boolean removeBias = true;
	protected boolean drawSamples = false;
	
	protected double highlightX1;
	protected double highlightX2;
	protected double viewStartTime;
	protected double viewEndTime; 
	
	protected boolean displayLabels = true;
	
	protected Color color = Color.BLUE;
	
	protected String yLabel = "Counts";
	
	protected String title;
	
	protected FrameDecorator decorator;
	
	public void setFrameDecorator(FrameDecorator fd)
	{
		decorator = fd;
	}
	
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
	
	public void setDrawSamples(boolean b)
	{
		drawSamples = b;
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
	
	public void setColor(Color c)
	{
		color = c;
	}
	
	public void setDisplayLabels(boolean b)
	{
		displayLabels = b;
	}
	
	public void setYLabel(String s)
	{
		yLabel = s;
	}

	public void setTitle(String s)
	{
		title = s;
	}
	
//	protected class DefaultFrameDecoratorSW implements FrameDecorator
//	{
//		public void decorate(FrameRenderer fr)
//		{
//			if (displayLabels)
//			{
//				int hTicks = graphWidth / 108;
//				int vTicks = graphHeight / 24;
//				fr.createDefaultAxis(hTicks, vTicks);
//				fr.setXAxisToTime(hTicks);
//				fr.getAxis().setLeftLabelAsText(yLabel, -52);
//				if (title != null)
//					fr.getAxis().setTopLabelAsText(title);
//				fr.getAxis().setBottomLeftLabelAsText("Time");
//			}
//			else
//			{
//				fr.createDefaultAxis(0, 0);
//			}		
//		}
//	}
	
	public void update()
	{
		if (decorator == null)
			decorator = new DefaultFrameDecorator();
		
		this.setExtents(viewStartTime, viewEndTime, minY, maxY);
		decorator.decorate(this);
	}
	
	public void render(Graphics2D g)
	{
		Shape origClip = g.getClip();
		
		if (axis != null)
			axis.render(g);
		
        g.clip(new Rectangle(graphX + 1, graphY + 1, graphWidth - 1, graphHeight - 1));

		double st = wave.getStartTime();
		double step = 1 / wave.getSamplingRate();
		wave.reset();
		
		double bias = 0;
		if (removeBias)
		    bias = wave.mean();
		
		g.setColor(color);
        
		double ns = (double)wave.samples() * (viewEndTime - viewStartTime) / (wave.getEndTime() - wave.getStartTime());
		double spp = ns / (double)graphWidth;
		Rectangle2D.Double box = new Rectangle2D.Double();
        if (spp < 50.0)
        {
        	GeneralPath gp = new GeneralPath();
			
			double y = wave.next();
			gp.moveTo((float)getXPixel(st), (float)(getYPixel(y - bias)));
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
					if (drawSamples && (1 / spp) > 2.0)
					{
						box.setRect((float)getXPixel(st) - 1.5, lastY - 1.5, 3, 3);
						g.draw(box);
					}
				}
			}
			g.draw(gp);
        }
        else
        {
        	double[][] spans = new double[graphWidth + 1][];
        	for (int i = 0; i < spans.length; i++)
        		spans[i] = new double[] { 1E300, -1E300 };
        	
        	double span = viewEndTime - viewStartTime;
        	
        	wave.reset();
        	double y;
        	int i;
        	while (wave.hasNext())
        	{
        		y = wave.next();
        		i = (int)(((st - viewStartTime) / span) * graphWidth + 0.5);
        		if (i >= 0 && i < spans.length && y != Wave.NO_DATA)
        		{
	        		spans[i][0] = Math.min(y, spans[i][0]);
	        		spans[i][1] = Math.max(y, spans[i][1]);
        		}
        		st += step;
        	}
        	
        	Line2D.Double line = new Line2D.Double();
        	double minY, maxY;
        	double lastMinY = -1E300;
        	double lastMaxY = 1E300;
        	for (i = 0; i < spans.length; i++)
        	{
        		minY = getYPixel(spans[i][0] - bias);
        		maxY = getYPixel(spans[i][1] - bias);
        		
        		if (maxY < lastMinY)
        		{
        			line.setLine(graphX + i - 1, lastMinY, graphX + i, maxY);
            		g.draw(line);
        		}
        		else if (minY > lastMaxY)
        		{
        			line.setLine(graphX + i - 1, lastMaxY, graphX + i, minY);
            		g.draw(line);
        		}
        		
        		line.setLine(graphX + i, minY, graphX + i, maxY);
        		g.draw(line);
        		lastMinY = minY;
        		lastMaxY = maxY;
        	}
        }
        
		g.setClip(origClip);
		
		if (axis != null)
			axis.postRender(g);
	}
}
