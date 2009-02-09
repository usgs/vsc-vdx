package gov.usgs.vdx.data.tilt;

import gov.usgs.util.Util;
import gov.usgs.vdx.data.GenericDataMatrix;
import gov.usgs.vdx.data.other.VoltageData;
import gov.usgs.vdx.data.other.RainfallData;
import gov.usgs.vdx.data.thermal.ThermalData;

import java.nio.ByteBuffer;
import java.util.List;

import cern.colt.matrix.DoubleFactory2D;
import cern.colt.matrix.DoubleMatrix2D;

public class TiltStationData extends GenericDataMatrix {
	
	private TiltData tiltData;
	private ThermalData holeTempData;
	private ThermalData boxTempData;
	private VoltageData instVoltData;
	private VoltageData gndVoltData;
	private RainfallData rainfallData;

	public TiltStationData()
	{}
	
	public TiltStationData(TiltData td, ThermalData htd, ThermalData btd, VoltageData ivd, VoltageData gvd, RainfallData rd) {
		tiltData		= td;
		holeTempData	= htd;
		boxTempData		= btd;
		instVoltData	= ivd;
		gndVoltData		= gvd;
		rainfallData	= rd;
	}

	public TiltStationData(List<double[]> pts) {
		
		DoubleMatrix2D tm	= DoubleFactory2D.dense.make(pts.size(), 3);
		DoubleMatrix2D htm	= DoubleFactory2D.dense.make(pts.size(), 2);
		DoubleMatrix2D btm	= DoubleFactory2D.dense.make(pts.size(), 2);
		DoubleMatrix2D ivm	= DoubleFactory2D.dense.make(pts.size(), 2);
		DoubleMatrix2D gvm	= DoubleFactory2D.dense.make(pts.size(), 2);
		DoubleMatrix2D rm	= DoubleFactory2D.dense.make(pts.size(), 2);
		
		double[] d;
		for (int i = 0; i < pts.size(); i++) {			
			d = pts.get(i);
			tm.setQuick(i, 0, d[0]); 
			htm.setQuick(i, 0, d[0]); 
			btm.setQuick(i, 0, d[0]);
			ivm.setQuick(i, 0, d[0]);
			gvm.setQuick(i, 0, d[0]);
			rm.setQuick(i, 0, d[0]);
			
			tm.setQuick(i, 1, d[1]);
			tm.setQuick(i, 2, d[2]);
			
			htm.setQuick(i, 1, d[3]);
			btm.setQuick(i, 1, d[4]);
			ivm.setQuick(i, 1, d[5]);
			gvm.setQuick(i, 1, d[6]);
			rm.setQuick(i, 1, d[7]);
		}
		
		tiltData		= new TiltData();
		tiltData.setData(tm);
		holeTempData	= new ThermalData();
		holeTempData.setData(htm);
		boxTempData		= new ThermalData();
		boxTempData.setData(btm);
		instVoltData	= new VoltageData();
		instVoltData.setData(ivm);
		gndVoltData		= new VoltageData();
		gndVoltData.setData(gvm);
		rainfallData	= new RainfallData();
		rainfallData.setData(rm);
	}
	
	public void setFromFullMatrix(DoubleMatrix2D dm) {
		
		DoubleMatrix2D tm	= DoubleFactory2D.dense.make(dm.rows(), 3);
		DoubleMatrix2D htm	= DoubleFactory2D.dense.make(dm.rows(), 2);
		DoubleMatrix2D btm	= DoubleFactory2D.dense.make(dm.rows(), 2);
		DoubleMatrix2D ivm	= DoubleFactory2D.dense.make(dm.rows(), 2);
		DoubleMatrix2D gvm	= DoubleFactory2D.dense.make(dm.rows(), 2);
		DoubleMatrix2D rm	= DoubleFactory2D.dense.make(dm.rows(), 2);
		
		for (int i = 0; i < dm.rows(); i++) {
			double t = dm.getQuick(i, 0);
			tm.setQuick(i, 0, t); 
			htm.setQuick(i, 0, t); 
			btm.setQuick(i, 0, t); 
			ivm.setQuick(i, 0, t); 
			gvm.setQuick(i, 0, t);
			rm.setQuick(i, 0, t);
			
			tm.setQuick(i, 1, dm.getQuick(i, 1));
			tm.setQuick(i, 2, dm.getQuick(i, 2));
			
			htm.setQuick(i, 1, dm.getQuick(i, 3));
			btm.setQuick(i, 1, dm.getQuick(i, 4));			
			ivm.setQuick(i, 1, dm.getQuick(i, 5));			
			gvm.setQuick(i, 1, dm.getQuick(i, 6));
			rm.setQuick(i, 1, dm.getQuick(i, 7));
		}
		
		tiltData		= new TiltData();
		tiltData.setData(tm);
		holeTempData	= new ThermalData();
		holeTempData.setData(htm);
		boxTempData		= new ThermalData();
		boxTempData.setData(btm);
		instVoltData	= new VoltageData();
		instVoltData.setData(ivm);
		gndVoltData		= new VoltageData();
		gndVoltData.setData(gvm);
		rainfallData	= new RainfallData();
		rainfallData.setData(rm);
	}

	public TiltData getTiltData() {
		return tiltData;
	}
	
	public ThermalData getHoleTempData() {
		return holeTempData;
	}
	
	public ThermalData getBoxTempData() {
		return boxTempData;
	}

	public VoltageData getInstVoltData() {
		return instVoltData;
	}

	public VoltageData getGndVoltData() {
		return gndVoltData;
	}
	
	public RainfallData getRainfallData() {
		return rainfallData;
	}

	public ByteBuffer toBinary() {
		int rows = tiltData.rows();
		int cols = 8;
		ByteBuffer bb = ByteBuffer.allocate(4 + (rows * cols) * 8);
		bb.putInt(rows);
		DoubleMatrix2D tm	= tiltData.getData();
		DoubleMatrix2D htm	= holeTempData.getData();
		DoubleMatrix2D btm	= boxTempData.getData();
		DoubleMatrix2D ivm	= instVoltData.getData();
		DoubleMatrix2D gvm	= gndVoltData.getData();
		DoubleMatrix2D rm	= rainfallData.getData();
		
		for (int i = 0; i < rows; i++) {
			bb.putDouble(tm.getQuick(i, 0));
			bb.putDouble(tm.getQuick(i, 1));
			bb.putDouble(tm.getQuick(i, 2));
			bb.putDouble(htm.getQuick(i, 1));
			bb.putDouble(btm.getQuick(i, 1));
			bb.putDouble(ivm.getQuick(i, 1));
			bb.putDouble(gvm.getQuick(i, 1));
			bb.putDouble(rm.getQuick(i, 1));
		}
		return bb;
	}

	public void fromBinary(ByteBuffer bb) {
		int rows = bb.getInt();
		int cols = ((bb.limit() - 4) / rows) / 8;
		DoubleMatrix2D data = DoubleFactory2D.dense.make(rows, cols);
		for (int i = 0; i < rows; i++) {
			for (int j = 0; j < cols; j++)
				data.setQuick(i, j, bb.getDouble());
		}
		setFromFullMatrix(data);
	}

	public String toCSV() {
		DoubleMatrix2D tm	= tiltData.getData();
		DoubleMatrix2D htm	= holeTempData.getData();
		DoubleMatrix2D btm	= boxTempData.getData();
		DoubleMatrix2D ivm	= instVoltData.getData();
		DoubleMatrix2D gvm	= gndVoltData.getData();
		DoubleMatrix2D rm	= rainfallData.getData();

		StringBuffer sb = new StringBuffer();
		for (int i = 0; i < tiltData.rows(); i++) {
			sb.append(Util.j2KToDateString(tm.getQuick(i, 0)) + ",");
			sb.append(tm.getQuick(i, 1) + ",");			
			sb.append(tm.getQuick(i, 2) + ",");	
			sb.append(htm.getQuick(i, 1) + ",");
			sb.append(btm.getQuick(i, 1) + ",");		
			sb.append(ivm.getQuick(i, 1) + ",");		
			sb.append(gvm.getQuick(i, 1) + ",");
			sb.append(rm.getQuick(i, 1) + "\n");
		}
			
		return sb.toString();
	}
}
