package gov.usgs.vdx.data.hypo;

import gov.usgs.plot.Jet;
import gov.usgs.plot.Renderer;
import gov.usgs.plot.SmartTick;
import gov.usgs.plot.Spectrum;
import gov.usgs.plot.Transformer;
import gov.usgs.vdx.data.hypo.Hypocenter;
import gov.usgs.vdx.data.hypo.HypocenterList;

import java.awt.BasicStroke;
import java.awt.Color;
import java.awt.Font;
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
 * of different ways. This is used by the catalog section of Valve.
 *
 * $Log: not supported by cvs2svn $
 * Revision 1.2  2005/01/21 21:45:01  cvs
 * Added color scale bar for triple view.
 *
 * Revision 1.1  2004/10/12 18:21:25  cvs
 * Initial commit.
 *
 * @author Dan Cervelli 
 */
public class HypocenterRenderer implements Renderer
{
    private HypocenterList data;
    private Transformer transformer;
    private String view;
    private static NumberFormat numberFormat;
    private String colorOpt;
    private double minTime;
    private double maxTime;
    private static final Spectrum spectrum = Jet.getInstance();
    private static final double[] scales = new double[] 
        {100000, 50000, 20000, 10000, 5000, 2000, 1000, 500, 200, 100, 50, 20, 10, 5, 2, 1, 0.5, 0.2, 0.1, 0.05, 0.02};     
    
    /** The shapes that used to render different magnitude earthquakes
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
    
    public static final int[] circleScaleOffset = new int[] {1, 8, 17, 28, 44, 64, 90, 120};
    
    // set these externally
	/** The colors for different depths
	 */
    public static Color[] colors = new Color[] {Color.BLACK, Color.GREEN, Color.RED, new Color(1.0f, 0.91f, 0.0f), Color.BLUE, new Color(0.8f, 0f, 1f), Color.BLACK};
	
	/** The depth intervals
	 */
    public static double[] depths = new double[] {10000, 0, -5, -10, -20, -40, -70, -10000};
    
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
	public HypocenterRenderer(HypocenterList d, Transformer fr, String v)
    {
        data = d;
        transformer = fr;
        view = v;
        if (numberFormat == null)
        {
            numberFormat = NumberFormat.getInstance();    
            numberFormat.setMaximumFractionDigits(0);
        }
        colorOpt = "D";
    }
    
	/** Sets the renderer to operate in color-time mode where time is 
	 * represented in color instead of depth.
	 * @param min the minimum time (j2ksec) for the coolest color
	 * @param max the maximum time (j2ksec) for the warmest color
	 */
    public void setColorTime(double min, double max)
    {
    	if (!view.equals("3"))
    		colorOpt = "T";
        minTime = min;
        maxTime = max;
    }
    
    public void setMonochrome()
    {
    	colorOpt = "M";
    }
    
    public void setColorDepth()
    {
    	colorOpt = "D";
    }
    
	/** Gets a renderer that draws the proper scale/key/legend for this
	 * HypocentererRenderer based on its current settings.  This uses the Jet
	 * color spectrum in some cases.
	 * @param xStart the x-pixel location for the scale
	 * @param yStart the y-pixel location for the scale
	 * @param kmPerPixel the value for the spatial scale
	 * @return a Renderer that draws the corrent scale/key/legend
	 */
    public Renderer getScaleRenderer(final double xStart, final double yStart, final double kmPerPixel, final boolean triple)
    {
        return new Renderer() 
        	{
	            
				/** The render function for the scale renderer.
				 * @param g the graphics object upon which to render
				 */
	            public void render(Graphics2D g)
	            {
	                Object aa = g.getRenderingHint(RenderingHints.KEY_ANTIALIASING);
	                Stroke oldStroke = g.getStroke();
	                g.setRenderingHint(RenderingHints.KEY_ANTIALIASING, RenderingHints.VALUE_ANTIALIAS_ON);
	                g.setColor(Color.BLACK);
	                g.setStroke(new BasicStroke(2.0f));
	
	                g.drawString("Magnitude", (float)xStart - 4, (float)yStart - 40);
	                Font origFont = g.getFont();
	                g.setFont(new Font("Arial", Font.PLAIN, 10));
	                for (int i = 0; i < HypocenterRenderer.circles.length; i++)
	                {
	                    //g.translate(xStart + 30 * i, HypocenterRenderer.circles[i].y + yStart);
	                    g.translate(xStart + circleScaleOffset[i], HypocenterRenderer.circles[i].y + yStart);
	                    g.setPaint(Color.WHITE);
	                    g.fill(HypocenterRenderer.circles[i]);
	                    g.setPaint(Color.BLACK);
	                    g.draw(HypocenterRenderer.circles[i]);
	                    //g.translate(-30 * i - xStart, -(HypocenterRenderer.circles[i].y + yStart));
	                    g.translate(-circleScaleOffset[i] - xStart, -(HypocenterRenderer.circles[i].y + yStart));
	                    //g.drawString("" + i, (float)xStart + 30 * i - 3, 15 + (float)yStart);
	                    g.drawString("" + i, (float)xStart + circleScaleOffset[i] - 3, 15 + (float)yStart);
	                }
	                Rectangle2D.Double rect = new Rectangle2D.Double();
	                //double xoff = 270 + xStart;
	                double xoff = 190 + xStart;
	                double yoff = yStart - 65;
	                g.setStroke(new BasicStroke(1.0f));
	                if (colorOpt.equals("D") || view.equals("3"))
	                {
	                	float dy = 0;
	                	if (view.equals("3"))
	                		dy = -60;
	                    for (int i = 1; i < depths.length - 2; i++)
	                    {
	                        rect.setRect(xoff - 15, yoff + dy, 10, 13);
	                        g.drawString("" + Math.round(-depths[i]), (float)xoff, (float)yoff + 4 + dy);
	                        g.setPaint(colors[i]);
	                        g.fill(rect);
	                        g.setPaint(Color.BLACK);
	                        g.draw(rect);
	                        yoff += 13;
	                    }
	                    g.drawString("" + Math.round(-depths[depths.length - 2]), (float)xoff, (float)yoff + 4 + dy);
	                    //g.drawString("Depth (km)", (float)xStart + 240, (float)yStart + 15);
	                    g.drawString("Depth (km)", (float)xStart + 160, (float)yStart + 15 + dy);
	                    
	                }
	                
	                xoff = 190 + xStart;
	                yoff = yStart - 65;
	                if (colorOpt.equals("T") || view.equals("3"))
	                {
	                	if (view.equals("3"))
	                		yoff += 45;
	                	spectrum.renderScale(g, xoff - 15, yoff, 10, 80, true, true);
	                    Object[] t = SmartTick.autoTimeTick(minTime, maxTime, 6);
	                    double[] ticks = (double[])t[0];
	                    String[] labels = (String[])t[1];
	                    double dt = maxTime - minTime;
	                    Line2D.Double line = new Line2D.Double();
	                    for (int i = 0; i < ticks.length; i++)
	                    {
	                    	double yl = ((ticks[i] - minTime) / dt) * 80;
	                        line.setLine(xoff - 5, yoff + yl, xoff - 3, yoff + yl);
	                        g.draw(line);
	                        g.drawString(labels[i], (float)xoff, (float)(yl + yoff + 3));
	                    }
	                }
	                g.setFont(origFont);
	                g.setStroke(new BasicStroke(2.0f));
	                if (kmPerPixel != 0.0)
	                {
	                    Line2D.Double line = new Line2D.Double();
	                    double len = 0;
	                    int index = -1;
	                    for (int i = 0; i < scales.length; i++)
	                        if (kmPerPixel * scales[i] < 150)
	                        {
	                            len = kmPerPixel * scales[i];
	                            index = i;
	                            break;
	                        }
	                    //line.setLine(xStart, yStart - 45, xStart + len, yStart - 45);
	                    line.setLine(xStart, yStart - 75, xStart + len, yStart - 75);
	                    g.draw(line);
	                    //g.drawString(scales[index] + " km", (float)xStart, (float)yStart - 49);
	                    g.drawString(scales[index] + " km", (float)xStart, (float)yStart - 79);
	                }
	                
	                if (view.equals("3"))
	                {
		                g.drawString("Top color scale for map view,", (float)xStart, (float)yStart + 35);
		                g.drawString("bottom for depth views.", (float)xStart, (float)yStart + 50);
	                }
	                g.setRenderingHint(RenderingHints.KEY_ANTIALIASING, aa);   
	                g.setStroke(oldStroke);
	            }
        	};
    }
    
	/** Renders according to the settings of this HypocenterRenderer.
	 * @param g the graphics object upon which to render
	 */
    public void render(Graphics2D g)
    {
        Color origColor = g.getColor();
        AffineTransform origAT = g.getTransform();
        Stroke origStroke = g.getStroke();
        
        Object aa = g.getRenderingHint(RenderingHints.KEY_ANTIALIASING);
        g.setRenderingHint(RenderingHints.KEY_ANTIALIASING, RenderingHints.VALUE_ANTIALIAS_ON);
        g.setStroke(new BasicStroke(2.0f));
        double xt = 0;
        double yt = 0;
        Color color = null;
        double dt = maxTime - minTime;
        List<Hypocenter> hcs = data.getHypocenters();
        for (int i = 0; i < hcs.size(); i++)
        {
        	Hypocenter hc = hcs.get(i);
            if (hc.getMag() != -99.99)
            {
                if (view.equals("M") || view.equals("3"))
                {
                    xt = transformer.getXPixel(hc.getLon());
                    yt = transformer.getYPixel(hc.getLat());
                }
                else if (view.equals("E") || view.equals("A"))
                {
                    xt = transformer.getXPixel(hc.getLon());
                    yt = transformer.getYPixel(hc.getDepth());
                }
                else if (view.equals("N"))
                {
                    xt = transformer.getXPixel(hc.getLat());
                    yt = transformer.getYPixel(hc.getDepth());
                } 
                else if (view.equals("NT"))
                {
                	xt = transformer.getXPixel(-hc.getDepth());
                	yt = transformer.getYPixel(hc.getLat());
                }
                else if (view.equals("T"))
                {
                    xt = transformer.getXPixel(hc.getTime());
                    yt = transformer.getYPixel(hc.getLon());
                }
                else if (view.equals("D"))
                {
                	xt = transformer.getXPixel(hc.getTime());
                    yt = transformer.getYPixel(hc.getDepth());
                }
                g.translate(xt, yt);
                
                if (colorOpt.equals("D"))
                {
                    for (int j = 0; j < depths.length - 1; j++)
                        if (hc.getDepth() <= depths[j] && hc.getDepth() > depths[j + 1])
                        {
                            color = colors[j];
                            break;
                        }
                }
                else if (colorOpt.equals("T"))
                {
                    int ci = (int)(((hc.getTime() - minTime) / dt) * ((double)spectrum.colors.length - 1));
                    color = spectrum.colors[ci];
                }
                else if (colorOpt.equals("M"))
                	color = Color.BLUE;
                
                g.setColor(color);
				int ci = (int)Math.floor(hc.getMag());
				if (ci < 0)
					ci = 0;
                g.draw(circles[ci]);
                g.translate(-xt, -yt);
            }
        }
        
        g.setRenderingHint(RenderingHints.KEY_ANTIALIASING, aa);
        g.setStroke(origStroke);
        g.setColor(origColor);
        g.setTransform(origAT);
    }
    
}
