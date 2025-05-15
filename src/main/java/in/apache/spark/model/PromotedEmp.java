package in.apache.spark.model;

import java.io.Serializable;

public class PromotedEmp implements Serializable{
	
	private int deptId;
	private int empBand;
	private double avgSalary;
	private double maxSalary;
	
	public int getDeptId() {
		return deptId;
	}
	public void setDeptId(int deptId) {
		this.deptId = deptId;
	}
	public int getEmpBand() {
		return empBand;
	}
	public void setEmpBand(int empBand) {
		this.empBand = empBand;
	}
	public double getAvgSalary() {
		return avgSalary;
	}
	public void setAvgSalary(double avgSalary) {
		this.avgSalary = avgSalary;
	}
	public double getMaxSalary() {
		return maxSalary;
	}
	public void setMaxSalary(double maxSalary) {
		this.maxSalary = maxSalary;
	}

	
}
