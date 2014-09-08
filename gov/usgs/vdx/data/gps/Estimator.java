//package gov.usgs.vdx.data.gps;
//
//import Jama.Matrix;
//
//public class Estimator {
//
//	public final Matrix A;
//	public final Matrix b;
//	public Matrix m;
//	public Matrix mcov;
//	public Matrix T;
//	
//	public double[] getErrorEllipseParameters()
//	{
//		return this.getErrorEllipseParameters(getChi2());
//	}
//	
//	public double[] getErrorEllipseParameters(double scale)
//	{
//		double result[] = new double[4];
//		
//		double[][] C = getModelCovariance();
//		int n = C.length - 3;
//
//		double e1 = scale * (C[0+n][0+n] - Math.sqrt(4*Math.pow(C[1+n][0+n],2) + Math.pow((C[0+n][0+n] - C[1+n][1+n]),2)) + C[1+n][1+n])/2;
//		double e2 =	scale * (C[0+n][0+n] + Math.sqrt(4*Math.pow(C[1+n][0+n],2) + Math.pow((C[0+n][0+n] - C[1+n][1+n]),2)) + C[1+n][1+n])/2;
//		
//		result[0] = Math.atan2(-(-C[0+n][0+n] + Math.sqrt(4*Math.pow(C[1+n][0+n],2) + Math.pow((C[0+n][0+n] - C[1+n][1+n]),2)) + C[1+n][1+n])/(2*C[1+n][0+n]),1);
//		result[1] = Math.max(e1, e2);
//		result[2] = Math.min(e1, e2);
//		result[3] = scale * C[2+n][2+n];
//		
//		return result;		
//		
//	}
//	
//	public double getChi2() {
//		
//		return Math.pow(A.getMatrix(0,b.getRowDimension()-1,0,m.getRowDimension()-1).times(m).minus(b).norm2(),2) / (b.getRowDimension() - m.getRowDimension());
//	}
//	
//	public double[] getModel() {
//	
//		return T.getMatrix(0,m.getRowDimension()-1,0,m.getRowDimension()-1).times(m).getRowPackedCopy();
//
//	}	
//	
//	public double[][] getModelCovariance() {		
//
//		return T.getMatrix(0,mcov.getRowDimension()-1,0,mcov.getRowDimension()-1).times(mcov).times(T.getMatrix(0,mcov.getRowDimension()-1,0,mcov.getRowDimension()-1).transpose()).getArray();
//	}
//
//	public void calculateVelocity() {
//		
//		m = A.solve(b);
//		mcov = A.transpose().times(A).inverse();
//
//	}
//	
//	public void calculateMean() {
//		
//		m = A.getMatrix(0,b.getRowDimension()-1,0,2).solve(b);
//		mcov = A.getMatrix(0,b.getRowDimension()-1,0,2).transpose().times(A.getMatrix(0,b.getRowDimension()-1,0,2)).inverse();
//	}
//
//	public void unSetOrigin() {
//		
//		T = Matrix.identity(6,6);
//	}
//
//	public void setOrigin(double lon, double lat) {
//		
//        double sinLon = Math.sin(Math.toRadians(lon));
//        double sinLat = Math.sin(Math.toRadians(lat));
//        double cosLon = Math.cos(Math.toRadians(lon));
//        double cosLat = Math.cos(Math.toRadians(lat));
//        T = new Matrix(new double[][] 
//        {
//            {          -sinLon,           cosLon,      0,                0,                0,      0 },
//            { -sinLat * cosLon, -sinLat * sinLon, cosLat,                0,                0,      0 },
//            {  cosLat * cosLon,  cosLat * sinLon, sinLat,                0,                0,      0 },
//            {                0,                0,      0,          -sinLon,           cosLon,      0 },
//            {                0,                0,      0, -sinLat * cosLon, -sinLat * sinLon, cosLat },
//            {                0,                0,      0,  cosLat * cosLon,  cosLat * sinLon, sinLat }
//        });
//		
//	}
//	
//	public Estimator(double[] t, double[] d, double[][] dcov) {
//
//		double[] w = new double[6];
//		int[] I = {0,1,2,1,2,2};
//		int[] J = {0,0,0,1,1,2};
//
//		A = new Matrix(d.length,6);
//		b = new Matrix(d.length,1);
//		
//		for (int i = 0; i < d.length; i = i + 3) {
//			
//			w[0] = 1/Math.sqrt(dcov[i][i]);
//			w[1] = -(dcov[i+1][i]/(dcov[i][i]*Math.sqrt(-(Math.pow(dcov[i+1][i], 2)/dcov[i][i]) + dcov[i+1][i+1])));
//			w[2] = (-(dcov[i+2][i]*dcov[i+1][i+1]) + dcov[i+1][i]*dcov[i+2][i+1])/((-Math.pow(dcov[i+1][i], 2) + 
//					dcov[i][i]*dcov[i+1][i+1])*Math.sqrt(-(Math.pow(dcov[i+2][i], 2)/dcov[i][i]) + dcov[i+2][i+2] - 
//					( Math.pow(-((dcov[i+1][i]*dcov[i+2][i])/dcov[i][i]) + dcov[i+2][i+1], 2)*(1/Math.sqrt(-(Math.pow(dcov[i+1][i], 2)/dcov[i][i]) + 
//					dcov[i+1][i+1])))/ Math.sqrt(-(Math.pow(dcov[i+1][i], 2)/dcov[i][i]) + dcov[i+1][i+1])));
//			w[3] = 1/Math.sqrt(-(Math.pow(dcov[i+1][i], 2)/dcov[i][i]) + dcov[i+1][i+1]);
//			w[4] = (dcov[i+1][i]*dcov[i+2][i] - dcov[i][i]*dcov[i+2][i+1])/((-Math.pow(dcov[i+1][i], 2) + 
//					dcov[i][i]*dcov[i+1][i+1])*Math.sqrt(-(Math.pow(dcov[i+2][i], 2)/dcov[i][i]) + dcov[i+2][i+2] - 
//					( Math.pow(-((dcov[i+1][i]*dcov[i+2][i])/dcov[i][i]) + dcov[i+2][i+1], 2)*(1/Math.sqrt(-(Math.pow(dcov[i+1][i], 2)/dcov[i][i]) + 
//					dcov[i+1][i+1])))/Math.sqrt(-(Math.pow(dcov[i+1][i], 2)/dcov[i][i]) + dcov[i+1][i+1])));
//			w[5] = 1/Math.sqrt(-(Math.pow(dcov[i+2][i], 2)/dcov[i][i]) + dcov[i+2][i+2] - 
//					( Math.pow(-((dcov[i+1][i]*dcov[i+2][i])/dcov[i][i]) + dcov[i+2][i+1], 2)*(1/Math.sqrt(-(Math.pow(dcov[i+1][i], 2)/dcov[i][i]) +
//					dcov[i+1][i+1])))/Math.sqrt(-(Math.pow(dcov[i+1][i], 2)/dcov[i][i]) + dcov[i+1][i+1]));
//						
//			for (int j = 0; j < 6; j++) {
//				A.set(i+I[j], J[j], w[j]);
//				A.set(i+I[j], J[j]+3, w[j] * (t[i/3] - t[0]));
//			}
//							
//			b.set(i, 0,   d[i] * w[0]);
//			b.set(i+1, 0, d[i] * w[1] + d[i+1] * w[3]);
//			b.set(i+2, 0, d[i] * w[2] + d[i+1] * w[4] + d[i+2] * w[5]);
//		}		
//			
//		unSetOrigin();
//		
//	}
//
//}