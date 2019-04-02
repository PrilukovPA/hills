package ru.evenx.hills;

@SuppressWarnings("serial")
public class HillsException extends Exception {

	public HillsException(String responseString) {
		super(responseString);
	}

	public HillsException(Exception e) {
		super(e);
	}

	public HillsException(String string, Throwable e) {
		super(string, e);
	}
}