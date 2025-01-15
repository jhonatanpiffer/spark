package com.virtualpairprogrammers;

public class IntegetWithSquareRoot {
	
	public Integer getOrignalNumber() {
		return orignalNumber;
	}

	public void setOrignalNumber(Integer orignalNumber) {
		this.orignalNumber = orignalNumber;
	}

	public Double getSqrt() {
		return sqrt;
	}

	public void setSqrt(Double sqrt) {
		this.sqrt = sqrt;
	}

	private Integer orignalNumber;
	private Double sqrt;

	public IntegetWithSquareRoot(Integer value) {
		this.orignalNumber = value;
		this.sqrt = Math.sqrt(orignalNumber);
	}

}
