package logreplay.consumer;

import logreplay.producer.Producer;

import org.apache.log4j.Logger;


public abstract class Consumer<T> implements Runnable {

	private static final Logger logger = Logger.getLogger(Consumer.class);
	private static final Logger statsLogger = Logger.getLogger("perfStats");

	public static class ConsumerException extends Exception {
		private static final long serialVersionUID = 1L;
		public ConsumerException(String msg, Throwable t) { 
			super(msg, t);
		}
		public ConsumerException(String msg) { 
			super(msg);
		}
	}
	
	private final Producer<T> producer;
	private long count = 0l;
	private long highTime = 0l;
	private long lowTime = Long.MAX_VALUE;
	private long runEndTime = 0l;
	private long runStartTime;
	private String threadCtx = null;
	
	private ExitStatus exitStatus = ExitStatus.running;
	
	public enum ExitStatus { 
		running,
		ok,
		error,
		interrupted
	}
	
	public Consumer(Producer<T> producer) {
		this.producer = producer;	
	}
	
	protected abstract long execute(T request) throws ConsumerException;

	public abstract void shutdown();
	
	protected Producer<T> getProducer() { 
		return this.producer;
	}
	
	/**
	 * For unit tests to be able to force entrance into the work loop.
	 * @return
	 */
	protected boolean hasNext() { 
		return !producer.isExhausted();
	}
	
	
	/**
	 * For unit tests to be able to test whether we enter the blocking take().
	 * @return
	 */
	protected T next() { 
		try { 
			return producer.take();
		} catch (InterruptedException ie) { 
			logger.warn("received interruption while blocking on take from producer");
			Thread.currentThread().interrupt();
			return null;			
		}
	}
	
	public void run() {
		threadCtx = Thread.currentThread().getName() + " (tid=" + Thread.currentThread().getId() + ")";
		try {
			runStartTime = System.currentTimeMillis();

			T request;
			long requestTotTime;
			while (hasNext()) {
				request = next();
				
				if (request == null) {
					logger.warn("null request!");
				} else { 
	 				count++;
	 				requestTotTime = execute(request);

					if (requestTotTime > highTime) { 
						highTime = requestTotTime;
					} else if (requestTotTime < lowTime) { 
						lowTime = requestTotTime;
					}
					
					if ((count % 1000) == 0) { 
						printStats();
					}
				}
			}
			
			if (Thread.interrupted()) { 
				Thread.currentThread().interrupt();
				exitStatus = ExitStatus.interrupted;
			} else { 
				exitStatus = ExitStatus.ok;
			}

		} catch (Exception e) { 
			logger.error("consumer threw an exception", e);
			exitStatus = ExitStatus.error;
	    } finally { 
			runEndTime = System.currentTimeMillis();
	    }
	}
	
	public ExitStatus getExitStatus() { 
		return exitStatus;
	}

	public long getTotalRequests() { return this.count; }

	public long getRunStartTime() { return this.runStartTime; }

	public long getRunEndTime() { return this.runEndTime; }

	public void printStats() { 
		long endTime = runEndTime > 0 ? runEndTime : System.currentTimeMillis();
		statsLogger.info("HttpGetConsumer " + this.toString() 
				+ " thread: " + threadCtx
				+ "; requests: " + count 
				+ ", runTime (ms): " + (endTime - runStartTime) 
				+ ", average (ms): " + (count > 0 && endTime > 0 && runStartTime > 0 ? ((endTime - runStartTime) / count) : 0) 
				+ ", high (ms): " + highTime 
				+ ", low (ms): " + lowTime);
	}

}