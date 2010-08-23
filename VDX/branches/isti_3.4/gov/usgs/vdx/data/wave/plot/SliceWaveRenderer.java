package gov.usgs.vdx.data.wave.plot;

import gov.usgs.plot.DefaultFrameDecorator;
import gov.usgs.plot.FrameDecorator;
import gov.usgs.plot.FrameRenderer;
import gov.usgs.plot.LegendRenderer;
import gov.usgs.plot.ShapeRenderer;
import gov.usgs.util.Util;
import gov.usgs.vdx.data.wave.SliceWave;
import gov.usgs.vdx.data.wave.Wave;

import java.awt.BasicStroke;
import java.awt.Color;
import java.awt.Graphics2D;
import java.awt.Rectangle;
import java.awt.Shape;
import java.awt.Stroke;
import java.awt.geom.GeneralPath;
import java.awt.geom.Line2D;
import java.awt.geom.Rectangle2D;

/**
 * A renderer for slice of wave time series.
 * 
 * $Log: not supported by cvs2svn $
 * Revision 1.15  2007/03/06 21:07:47  tparker
 * tweak title catch
 *
 * Revision 1.14  2007/03/06 20:05:43  tparker
 * Gracefully deal with lack of title
 *
 * Revision 1.13  2007/03/06 17:53:00  cervelli
 * Renders ylabel on update
 *
 * Revision 1.12  2007/02/27 20:07:48  cervelli
 * Added support for turning calibration use on and off.
 *
 * Revision 1.11  2006/10/26 01:04:44  dcervelli
 * Changes for labeling.
 *
 * Revision 1.10  2006/07/25 16:23:22  cervelli
 * Changes for new DefaultFrameDecorator.
 *
 * Revision 1.9  2006/07/22 20:15:45  cervelli
 * Interim changes for conversion to FrameDecorators.
 *
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
	protected SliceWave wave;
	
	protected boolean autoScale = true;
	protected boolean removeBias = true;
	protected boolean drawSamples = false;
	
	protected double highlightX1;
	protected double highlightX2;
	protected double viewStartTime;
	protected double viewEndTime; 
	protected String timeZone;
	
	protected Color color = Color.BLUE;
	
	protected String yLabelText = "";
	
	protected String title;
	
	protected FrameDecorator decorator;
	
	public boolean xTickMarks = true;
	public boolean xTickValues = true;
	public boolean xUnits = true;
	public boolean xLabel = false;
	public boolean yTickMarks = true;
	public boolean yTickValues = true;
	public boolean yUnits = true;
	public boolean yLabel = false;
	
	/**
	 * Set frame decorator to draw graph's frame
	 */
	public void setFrameDecorator(FrameDecorator fd)
	{
		decorator = fd;
	}

	/**
	 * Set highlighted zone
	 * @param x1 minimum x
	 * @param x2 maximum X
	 */
	public void setHighlight(double x1, double x2)
	{
		highlightX1 = x1;
		highlightX2 = x2;
	}
	
	/**
	 * Get maximum Y value
	 */
	public double getMaxY() 
	{
		return maxY;
	}

	/**
	 * Set maximum Y value
	 */
	public void setMaxY(double maxY) 
	{
		this.maxY = maxY;
	}
	
	/**
	 * Get minimum Y value
	 */
	public double getMinY() 
	{
		return minY;
	}
	
	/**
	 * Set minimum Y value
	 */
	public void setMinY(double minY) 
	{
		this.minY = minY;
	}
	
	/**
	 * Get autoscale flag
	 */
	public boolean isAutoScale() 
	{
		return autoScale;
	}

	/**
	 * Get demean flag
	 */
	public boolean isRemoveBias() 
	{
		return removeBias;
	}

	/**
	 * Set autoscale flag
	 */
	public void setAutoScale(boolean b)
	{
		autoScale = b;
	}

	/**
	 * Set limits on Y axis
	 */
	public void setYLimits(double min, double max)
	{
		minY = min;
		maxY = max;
	}
	
	/**
	 * Set demean flag
	 */
	public void setRemoveBias(boolean b)
	{
	    removeBias = b;
	}
	
	/**
	 * Set draw samples flag
	 */
	public void setDrawSamples(boolean b)
	{
		drawSamples = b;
	}
	
	/**
	 * Set slice to render
	 */
	public void setWave(SliceWave w)
	{
		wave = w;
	}

	/**
	 * Set limits on time axis
	 * @param t1 start time
	 * @param t2 end time
	 */
	public void setViewTimes(double t1, double t2, String timeZone)
	{
	    viewStartTime = t1;
	    viewEndTime = t2;
	    this.timeZone = timeZone;
	}
	
	/**
	 * Set color
	 */
	public void setColor(Color c)
	{
		color = c;
	}
	
	/**
	 * Set Y axis label
	 */
	public void setYLabelText(String s)
	{
		yLabelText = s;
	}

	/**
	 * Set graph title
	 */
	public void setTitle(String s)
	{
		title = s;
	}

	/**
	 * Create default decorator to render frame
	 */
	public void createDefaultFrameDecorator()
	{
		decorator = new DefaultWaveFrameDecorator();
	}
	
	/** Creates a standard legend, a small line and point sample followed by
	 * the specified names.
	 * @param s the legend names
	 */
    public void createDefaultLegendRenderer(String[] s)
    {
        setLegendRenderer(new LegendRenderer());
        getLegendRenderer().x = graphX + 6;
        getLegendRenderer().y = graphY + 6;
        ShapeRenderer sr = new ShapeRenderer(new GeneralPath(GeneralPath.WIND_NON_ZERO, 1));
        sr.antiAlias		= true;
        sr.color			= color;
        sr.stroke			= new BasicStroke();
        for (int i = 0; i < s.length; i++) 
            if (s[i] != null)
            {
                 getLegendRenderer().addLine(sr, null, s[i]);
            }
    }
	
	protected class DefaultWaveFrameDecorator extends DefaultFrameDecorator
	{
		public DefaultWaveFrameDecorator()
		{
			if(yUnits){
				this.yAxisLabel = yLabelText;
			}
			if(xUnits){
				this.xAxisLabel = timeZone + " Time (" + Util.j2KToDateString(viewStartTime, "yyyy MM dd") + " to " + Util.j2KToDateString(viewEndTime, "yyyy MM dd")+ ")";
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
			title = SliceWaveRenderer.this.title;
			titleBackground = Color.WHITE;
			// TODO: should probably have x-axis label be "time"
		}
	}
	
	/**
	 * Reinitialize frame decorator with this renderer data
	 */
	public void update()
	{
		if (decorator == null)
			createDefaultFrameDecorator();
		if (decorator instanceof DefaultFrameDecorator)
			   ((DefaultFrameDecorator)decorator).yAxisLabel = yLabelText;
		this.setExtents(viewStartTime, viewEndTime, minY, maxY);
		decorator.decorate(this);
	}
	
	/**
	 * Render slice graph
	 */
	public void render(Graphics2D g)
	{
	    Color origColor			= g.getColor();
	    Stroke origStroke		= g.getStroke();
	    Shape origClip			= g.getClip();
		
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
        
        if (getLegendRenderer() != null) {
    		g.setColor(Color.BLACK);
            getLegendRenderer().render(g);
        }
		g.setClip(origClip);
		
		if (axis != null)
			axis.postRender(g);
		g.setStroke(origStroke);
	    g.setColor(origColor);
	}
}
