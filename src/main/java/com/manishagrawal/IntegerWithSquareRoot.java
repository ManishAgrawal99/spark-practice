package com.manishagrawal;

public class IntegerWithSquareRoot {
	
	private int origialNumber;
	@SuppressWarnings("unused")
	private double squareRoot;

	public IntegerWithSquareRoot(int i) {
		this.origialNumber = i;
		this.squareRoot = Math.sqrt(origialNumber);
	}

}
