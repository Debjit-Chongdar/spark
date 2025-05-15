package in.apache.spark.model;

import java.io.Serializable;
import java.util.List;

public class Emp implements Serializable{

	private Long id;
	private String name;
	private Double salary;
	private Long deptId;
	private List<EmpAddress> address;
	
	public Long getId() {
		return id;
	}
	public void setId(Long id) {
		this.id = id;
	}
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public Double getSalary() {
		return salary;
	}
	public void setSalary(Double salary) {
		this.salary = salary;
	}
	public Long getDeptId() {
		return deptId;
	}
	public void setDeptId(Long deptId) {
		this.deptId = deptId;
	}
	public List<EmpAddress> getAddress() {
		return address;
	}
	public void setAddress(List<EmpAddress> empAddress) {
		this.address = empAddress;
	}
	
	
}
