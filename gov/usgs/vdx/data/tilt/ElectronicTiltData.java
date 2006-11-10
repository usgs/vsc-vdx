package gov.usgs.vdx.data.tilt;

import gov.usgs.util.Util;
import gov.usgs.vdx.data.BinaryDataSet;
import gov.usgs.vdx.data.other.VoltageData;
import gov.usgs.vdx.data.thermal.ThermalData;

import java.nio.ByteBuffer;
import java.util.List;

import cern.colt.matrix.DoubleFactory2D;
import cern.colt.matrix.DoubleMatrix2D;

/**
 * 
 * $Log: not supported by cvs2svn $
 * Revision 1.1  2005/10/14 20:44:07  dcervelli
 * Initial commit.
 *
 * @author Dan Cervelli
 */
public class ElectronicTiltData implements BinaryDataSet
{
	private TiltData tiltData;
	private ThermalData thermalData;
	private VoltageData voltageData;

	public ElectronicTiltData()
	{}
	
	public ElectronicTiltData(TiltData td, ThermalData thd, VoltageData vd)
	{
		tiltData = td;
		thermalData = thd;
		voltageData = vd;
	}

	public ElectronicTiltData(List<double[]> pts)
	{
		DoubleMatrix2D tm = DoubleFactory2D.dense.make(pts.size(), 3);
		DoubleMatrix2D thm = DoubleFactory2D.dense.make(pts.size(), 2);
		DoubleMatrix2D vm = DoubleFactory2D.dense.make(pts.size(), 2);
		
		double[] d;
		for (int i = 0; i < pts.size(); i++)
		{
			d = pts.get(i);
			tm.setQuick(i, 0, d[0]); 
			thm.setQuick(i, 0, d[0]); 
			vm.setQuick(i, 0, d[0]);
			
			tm.setQuick(i, 1, d[1]);
			tm.setQuick(i, 2, d[2]);
			
			vm.setQuick(i, 1, d[3]);
			thm.setQuick(i, 1, d[4]);
		}
		
		tiltData = new TiltData();
		tiltData.setData(tm);
		thermalData = new ThermalData();
		thermalData.setData(thm);
		voltageData = new VoltageData();
		voltageData.setData(vm);
	}
	
	public void setFromFullMatrix(DoubleMatrix2D dm)
	{
		DoubleMatrix2D tm = DoubleFactory2D.dense.make(dm.rows(), 3);
		DoubleMatrix2D thm = DoubleFactory2D.dense.make(dm.rows(), 2);
		DoubleMatrix2D vm = DoubleFactory2D.dense.make(dm.rows(), 2);
		
		for (int i = 0; i < dm.rows(); i++)
		{
			double t = dm.getQuick(i, 0);
			tm.setQuick(i, 0, t); 
			thm.setQuick(i, 0, t); 
			vm.setQuick(i, 0, t);
			
			tm.setQuick(i, 1, dm.getQuick(i, 1));
			tm.setQuick(i, 2, dm.getQuick(i, 2));
			
			vm.setQuick(i, 1, dm.getQuick(i, 3));
			thm.setQuick(i, 1, dm.getQuick(i, 4));
		}
		
		tiltData = new TiltData();
		tiltData.setData(tm);
		thermalData = new ThermalData();
		thermalData.setData(thm);
		voltageData = new VoltageData();
		voltageData.setData(vm);
	}
	
	public ThermalData getThermalData()
	{
		return thermalData;
	}

	public TiltData getTiltData()
	{
		return tiltData;
	}

	public VoltageData getVoltageData()
	{
		return voltageData;
	}

	public ByteBuffer toBinary()
	{
		int rows = tiltData.rows();
		int cols = 5;
		ByteBuffer bb = ByteBuffer.allocate(4 + (rows * cols) * 8);
		bb.putInt(rows);
		DoubleMatrix2D tm = tiltData.getData();
		DoubleMatrix2D thm = thermalData.getData();
		DoubleMatrix2D vm = voltageData.getData();
		
		for (int i = 0; i < rows; i++)
		{
			bb.putDouble(tm.getQuick(i, 0));
			bb.putDouble(tm.getQuick(i, 1));
			bb.putDouble(tm.getQuick(i, 2));
			bb.putDouble(vm.getQuick(i, 1));
			bb.putDouble(thm.getQuick(i, 1));
		}
		return bb;
	}

	public void fromBinary(ByteBuffer bb)
	{
		int rows = bb.getInt();
		int cols = ((bb.limit() - 4) / rows) / 8;
		DoubleMatrix2D data = DoubleFactory2D.dense.make(rows, cols);
		for (int i = 0; i < rows; i++)
		{
			for (int j = 0; j < cols; j++)
				data.setQuick(i, j, bb.getDouble());
		}
		setFromFullMatrix(data);
	}

	public String toCSV()
	{
		DoubleMatrix2D tm = tiltData.getData();
		DoubleMatrix2D thm = thermalData.getData();
		DoubleMatrix2D vm = voltageData.getData();

		StringBuffer sb = new StringBuffer();
		for (int i = 0; i < tiltData.rows(); i++)
		{
			sb.append(Util.j2KToDateString(tm.getQuick(i, 0)) + ",");
			sb.append(tm.getQuick(i, 1) + ",");			
			sb.append(vm.getQuick(i, 1) + ",");
			sb.append(thm.getQuick(i, 1));
		}
			
		return sb.toString();
	}
}
