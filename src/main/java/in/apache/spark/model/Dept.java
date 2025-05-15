package in.apache.spark.model;

import java.io.Serializable;

public class Dept implements Serializable{

	private Long id;
	private String deptName;
	public Long getId() {
		return id;
	}
	public void setId(Long id) {
		this.id = id;
	}
	public String getDeptName() {
		return deptName;
	}
	public void setDeptName(String deptName) {
		this.deptName = deptName;
	}
	
	
}
