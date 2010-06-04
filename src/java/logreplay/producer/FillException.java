package logreplay.producer;

/**
 * Thrown to indicate that an exception occurred while filling a work queue in a Processor.
 * @author cwoerner
 *
 */
public class FillException extends Exception { 

	private static final long serialVersionUID = 1L;

	public FillException(Throwable t) { 
		super(t);
	}
	
	public FillException(String msg, Throwable t) { 
		super(msg, t);
	}
	
	public FillException(String msg) { 
		super(msg);
	}
}
