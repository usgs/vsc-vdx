package gov.usgs.vdx.data.hypo.plot;

import gov.usgs.plot.ArbDepthCalculator;
import gov.usgs.plot.Jet;
import gov.usgs.plot.Renderer;
import gov.usgs.plot.ArbDepthFrameRenderer;
import gov.usgs.plot.SmartTick;
import gov.usgs.plot.Spectrum;
import gov.usgs.plot.Transformer;
import gov.usgs.vdx.data.hypo.Hypocenter;
import gov.usgs.vdx.data.hypo.HypocenterList;

import java.awt.BasicStroke;
import java.awt.Color;
import java.awt.Font;
import java.awt.FontMetrics;
import java.awt.Graphics2D;
import java.awt.RenderingHints;
import java.awt.Stroke;
import java.awt.geom.AffineTransform;
import java.awt.geom.Ellipse2D;
import java.awt.geom.Line2D;
import java.awt.geom.Rectangle2D;
import java.text.NumberFormat;
import java.util.List;

/**
 * HypocenterRenderer is a class that render a list of hypocenters in a variety
 * of different ways.
 *
 * @author Dan Cervelli 
 */
public class HypocenterRenderer implements Renderer
{
	/**
	 * Enumeration to define scale type
	 */
	public enum AxesOption
	{
		MAP_VIEW, LON_DEPTH, LAT_DEPTH, TIME_DEPTH, TRIPLE_VIEW, ARB_DEPTH, ARB_TIME;
		
		public static AxesOption fromString(String c)
		{
			if (c == null)
				return null;
			switch(c.charAt(0))
			{
				case 'M':
					return MAP_VIEW;
				case 'E':
					return LON_DEPTH;
				case 'N':
					return LAT_DEPTH;
				case 'D':
					return TIME_DEPTH;
				case 'A':				
					return ARB_DEPTH;
				case 'T':				
					return ARB_TIME;
				case '3':
					return TRIPLE_VIEW;

				default:
					return null;
			}
		}
	}
	
	/**
	 * Enumeration to define coloring mode
	 */
	public enum ColorOption
	{
		DEPTH, TIME, MONOCHROME;
		
		public static ColorOption fromString(String c)
		{
			if (c == null)
				return null;
			switch (c.charAt(0))
			{
				case 'D':
					return DEPTH; //Coloring by depth
				case 'T':
					return TIME;  //Coloring by time
				case 'M':
					return MONOCHROME;
				default:
					return null;
			}
		}
		
		public static ColorOption chooseAuto(AxesOption ax)
		{
			if (ax == null)
				return null;

			switch (ax)
			{
			case ARB_TIME:
			case MAP_VIEW:
				return DEPTH;

			case LON_DEPTH:
			case LAT_DEPTH:
			case ARB_DEPTH:
				return TIME;

			default:
				return MONOCHROME;
			}
		}
	}
	
    private HypocenterList data;
    private Transformer transformer;
    private AxesOption axesOption;
    private static NumberFormat numberFormat;
    private ColorOption colorOption;
    private double minTime;
    private double maxTime;
    private static final Spectrum spectrum = Jet.getInstance();

    private Renderer colorScaleRenderer;
    private Renderer magnitudeScaleRenderer;
    
    /** 
     * The shapes that used to render different magnitude earthquakes
	 */
    public static Ellipse2D.Float[] circles = new Ellipse2D.Float[]
    {
        new Ellipse2D.Float(-1.5f, -1.5f, 3, 3),
        new Ellipse2D.Float(-3, -3, 6, 6),
        new Ellipse2D.Float(-5.5f, -5.5f, 11, 11),
        new Ellipse2D.Float(-9, -9, 18, 18),
        new Ellipse2D.Float(-13.5f, -13.5f, 27, 27),
        new Ellipse2D.Float(-19, -19, 38, 38),
        new Ellipse2D.Float(-25.5f, -25.5f, 51, 51),
        new Ellipse2D.Float(-34f, -34f, 68, 68),
    };
    
    public static final int[] circleScaleOffset = new int[] {0, 30, 60, 90, 120, 150, 180, 210};
    
	/** 
	 * The colors for different depths
	 */
    public static Color[] colors = new Color[] {Color.BLACK, Color.GREEN, Color.RED, new Color(1.0f, 0.91f, 0.0f), Color.BLUE, new Color(0.8f, 0f, 1f)};
	
	/** 
	 * The depth intervals
	 */
    public static double[] depths = new double[] {-10000, 0, 5, 13, 20, 40, 10000};
    public static String[] depthStrings = new String[] {"", "0", "5", "13", "20", "40", "70+"};
    
	/** Constructor for HypocenterRenderer that gets the data, the coordinate
	 * transformer and the view type.  The different views are:<br>
	 * M: map view (axes=lon/lat)<br>
	 * E or A: east view (axes=lon/depth)<br>
	 * N: north view (axes=lat/depth)<br>
	 * T: time view (axes=time/lon)<br>
	 * @param d the earthquake data
	 * @param fr the coordinate transformer (usually a FrameRenderer)
	 * @param v the view type 
	 */
	public HypocenterRenderer(HypocenterList d, Transformer fr, AxesOption v)
    {
        data = d;
        transformer = fr;
        axesOption = v;
        if (numberFormat == null)
        {
            numberFormat = NumberFormat.getInstance();    
            numberFormat.setMaximumFractionDigits(0);
        }
        colorOption = ColorOption.chooseAuto(v);
    }
    
	/** 
	 * Sets the renderer to operate in color-time mode where time is 
	 * represented in color instead of depth.
	 * @param min the minimum time (j2ksec) for the coolest color
	 * @param max the maximum time (j2ksec) for the warmest color
	 */
    public void setColorTime(double min, double max)
    {
//    	if (axes == Axes.TRIPLE_VIEW)
//    		colorOption = "T";
    	colorOption = ColorOption.TIME;
        minTime = min;
        maxTime = max;
    }
 
    /**
     * Sets monochrome color mode
     */
    public void setMonochrome()
    {
    	colorOption = ColorOption.MONOCHROME;
    }
    
    /**
     * Sets depth color mode
     */
    public void setColorDepth()
    {
    	colorOption = ColorOption.DEPTH;
    }
    
    /**
     * Sets color mode
     * @param c Color option to set
     */
    public void setColorOption(ColorOption c)
    {
    	colorOption = c;
    }
    
    /**
     * Create renderer to draw hypocenter's magnitude
     * @param xStart hypocenter's X pixel coordinate
     * @param yStart hypocenter's Y pixel coordinate
     */
    public void createMagnitudeScaleRenderer(final double xStart, final double yStart)
    {
    	magnitudeScaleRenderer = new Renderer()
	    	{
	    		public void render(Graphics2D g)
	    		{
	    			g.setRenderingHint(RenderingHints.KEY_ANTIALIASING, RenderingHints.VALUE_ANTIALIAS_ON);
	                g.setColor(Color.BLACK);
	                g.setStroke(new BasicStroke(2.0f));
	                g.setFont(new Font("Arial", Font.PLAIN, 10));
	                FontMetrics metrics = g.getFontMetrics(g.getFont());
	
	                g.drawString("Magnitude", (float)xStart - 80, (float)yStart);
	                
	    			for (int i = 0; i < HypocenterRenderer.circles.length; i++)
	                {
	                    g.translate(xStart - HypocenterRenderer.circles[i].x + circleScaleOffset[i], yStart + HypocenterRenderer.circles[i].y - 15);
	                    g.setPaint(Color.WHITE);
	                    g.fill(HypocenterRenderer.circles[i]);
	                    g.setPaint(Color.BLACK);
	                    g.draw(HypocenterRenderer.circles[i]);
	                    g.drawString("" + i, -(metrics.stringWidth("" + i) / 2), -HypocenterRenderer.circles[i].y + 15);
	                    g.translate(-(xStart - HypocenterRenderer.circles[i].x  + circleScaleOffset[i]), -(yStart + HypocenterRenderer.circles[i].y - 15));
	                }	
	    		}
	    	};
    }
    
    /**
     * Create renderer to draw hypocenter in current color mode and scale type
     * @param xStart hypocenter's X pixel coordinate
     * @param yStart hypocenter's Y pixel coordinate
     */
    public void createColorScaleRenderer(final double xStart, final double yStart)
    {
    	colorScaleRenderer = new Renderer()
	    	{
	    		public void render(Graphics2D g)
	    		{
	    			g.setStroke(new BasicStroke(1.0f));
	                g.setFont(new Font("Arial", Font.PLAIN, 10));
	    			if (colorOption == ColorOption.DEPTH)
	                {
	    				Rectangle2D.Double rect = new Rectangle2D.Double();
	                	double yoff = 0;
	                	double ty = (depths.length - 2) * 18;
	                	
	                    g.drawString("Depth (km)", (float)xStart + 60, (float)yStart);
	                    
	                    for (int i = 1; i < depths.length - 1; i++)
	                    {
	                        rect.setRect(xStart, yStart - ty + yoff, 10, 18);
	                        g.drawString(depthStrings[i], (float)xStart + 18, (float)(yStart - ty + 4 + yoff));
	                        g.setPaint(colors[i]);
	                        g.fill(rect);
	                        g.setPaint(Color.BLACK);
	                        g.draw(rect);
	                        yoff += 18;
	                    }
	                    g.drawString(depthStrings[depths.length - 1], (float)xStart + 18, (float)yStart);
	                    
	                }
	    			
	                if (colorOption == ColorOption.TIME)
	                {
	                	spectrum.renderScale(g, xStart, yStart - 90, 10, 90, true, true);
	                    Object[] t = SmartTick.autoTimeTick(minTime, maxTime, 5);
	                    double[] ticks = (double[])t[0];
	                    String[] labels = (String[])t[1];
	                    double dt = maxTime - minTime;
	                    Line2D.Double line = new Line2D.Double();
	                    
	                    g.drawString("Time", (float)xStart + 60, (float)yStart);
	                    
	                    for (int i = 0; i < ticks.length; i++)
	                    {
	                    	double yl = ((ticks[i] - minTime) / dt) * 90;
	                        line.setLine(xStart + 10, yStart - 90 + yl, xStart + 13, yStart - 90 + yl);
	                        g.draw(line);
	                        g.drawString(labels[i], (float)xStart + 16, (float)(yl + yStart - 90 + 3));
	                    }
	                }
	    		}
	    	};
    }
    
	/** 
	 * Renders according to the settings of this HypocenterRenderer.
	 * @param g the graphics object upon which to render
	 */
    public void render(Graphics2D g)
    {
        Color origColor = g.getColor();
        AffineTransform origAT = g.getTransform();
        Stroke origStroke = g.getStroke();
        Font origFont = g.getFont();
        Object aa = g.getRenderingHint(RenderingHints.KEY_ANTIALIASING);
        g.setRenderingHint(RenderingHints.KEY_ANTIALIASING, RenderingHints.VALUE_ANTIALIAS_ON);
        g.setStroke(new BasicStroke(2.0f));
        double xt = 0;
        double yt = 0;
        Color color = null;
        double dt = maxTime - minTime;
        List<Hypocenter> hcs = data.getHypocenters();
        ArbDepthFrameRenderer adfr = null;
        
        int skippedCount = 0;
        
        for (int i = 0; i < hcs.size(); i++)
        {
        	Hypocenter hc = hcs.get(i);
            if (hc.prefmag != -99.99)
            {
            	switch(axesOption)
            	{
	            	case MAP_VIEW:
	            	case TRIPLE_VIEW:
	            		xt = transformer.getXPixel(hc.lon);
	                    yt = transformer.getYPixel(hc.lat);
	                    break;
	            	case LON_DEPTH:
	            		xt = transformer.getXPixel(hc.lon);
	                    yt = transformer.getYPixel(hc.depth);
	                    break;
	            	case LAT_DEPTH:
	            		xt = transformer.getXPixel(hc.lat);
	                    yt = transformer.getYPixel(hc.depth);
	            		break;
	            	case ARB_DEPTH:
	            		// this is where we must convert from lat/lon to distance along a line.
	            		// for the xt point.
	            	
	            		// if my transformer is really of type ArbDepthFrameRenderer, then use the embedded ArbDepthCalculator
	            		adfr = null;
	            		
	            		
	            		if (transformer instanceof ArbDepthFrameRenderer) {	            		
	            			adfr = (ArbDepthFrameRenderer)transformer;
	            			ArbDepthCalculator adc = adfr.getArbDepthCalc();

	            			if (adc != null) {

	            				double projectedDist = adc.getScaledProjectedDistance(hc.lat, hc.lon);	        						

	            				if (projectedDist < 0) {
	            					skippedCount++;
	            					continue;
	            				}
	            			
	            				if (projectedDist > adc.getMaxDist()) {
	            					skippedCount++;
	            					continue;
	            				}

	            				double projectedWidth = Math.abs(adc.getScalePojectedWidth(hc.lat, hc.lon));
	            				double tmpWidth = adc.getWidth();
	            				if (tmpWidth > 0.0) {
	            					if (projectedWidth > tmpWidth) {
	            						skippedCount++;
	            						continue;
	            					}
	            				}

	            				xt = transformer.getXPixel(projectedDist);
	            			} else {
	            				// we don't have the arbitrary depth calculator, just plot the point at zero.
	            				xt = 0.0;
	            			}
	            				
	        				
	            		} else {		            				            			
	            			xt = transformer.getXPixel(hc.lat);
	            		}
	                    yt = transformer.getYPixel(hc.depth);
	            		break;
	            	case TIME_DEPTH:
	            		xt = transformer.getXPixel(hc.j2ksec);
	                    yt = transformer.getYPixel(hc.depth);
	            		break;

	            	case ARB_TIME:
	            		// this is where we must convert from lat/lon to distance along a line.
	            		// for the xt point.

	            		// if my transformer is really of type ArbDepthFrameRenderer, then use the embedded ArbDepthCalculator
	            		adfr = null;


	            		if (transformer instanceof ArbDepthFrameRenderer) {	            		
	            			adfr = (ArbDepthFrameRenderer)transformer;
	            			ArbDepthCalculator adc = adfr.getArbDepthCalc();

	            			if (adc != null) {

	            				double projectedDist = adc.getScaledProjectedDistance(hc.lat, hc.lon);	        						

	            				if (projectedDist < 0) {
	            					skippedCount++;
	            					continue;
	            				}

	            				if (projectedDist > adc.getMaxDist()) {
	            					skippedCount++;
	            					continue;
	            				}

	            				double projectedWidth = Math.abs(adc.getScalePojectedWidth(hc.lat, hc.lon));
	            				double tmpWidth = adc.getWidth();
	            				if (tmpWidth > 0.0) {
	            					if (projectedWidth > tmpWidth) {
	            						skippedCount++;
	            						continue;
	            					}
	            				}
	            				
	            				yt = transformer.getYPixel(projectedDist);
	            			} else {
	            				// we don't have the arbitrary depth calculator, just plot the point at zero.
	            				yt = 0.0;
	            			}


	            		} else {		            				            			
	            			yt = transformer.getXPixel(hc.lat);
	            		}
	            		xt = transformer.getXPixel(hc.j2ksec);
	            		break;

            	}
                g.translate(xt, yt);
                
                switch(colorOption)
                {
	                case DEPTH:
	                	for (int j = 0; j < depths.length - 1; j++)
	                	{
	                        if (hc.depth >= depths[j] && hc.depth < depths[j + 1])
	                        {
	                            color = colors[j];
	                            break;
	                        }
	                	}
	                	break;
	                case TIME:
	                	int ci = (int)(((hc.j2ksec - minTime) / dt) * ((double)spectrum.colors.length - 1));
	                    color = spectrum.colors[ci];
	                	break;
	                case MONOCHROME:
	                	color = Color.BLUE;
	                	break;
                }
                
                g.setColor(color);
				int ci = (int)Math.floor(hc.prefmag);
				if (ci < 0)
					ci = 0;
                g.draw(circles[ci]);
                g.translate(-xt, -yt);
            }
        }

        g.setRenderingHint(RenderingHints.KEY_ANTIALIASING, aa);
        
        g.setColor(origColor);
        g.setTransform(origAT);
        
        if (colorScaleRenderer != null)
        	colorScaleRenderer.render(g);
        if (magnitudeScaleRenderer != null)
        	magnitudeScaleRenderer.render(g);
        
        g.setStroke(origStroke);
        g.setFont(origFont);
    }
    
}
