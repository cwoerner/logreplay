package logreplay;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Properties;

import logreplay.consumer.Consumer;
import logreplay.consumer.HttpGetConsumer;
import logreplay.producer.FillException;
import logreplay.producer.Producer;

import org.apache.http.client.methods.HttpGet;
import org.apache.log4j.Logger;


public abstract class HttpGetRequestRunner implements Runnable {

	private static final Logger logger = Logger.getLogger(HttpGetRequestRunner.class);

	private final int numThreads;
	
	private List<Consumer<HttpGet>> httpGetRequestors = null;
	private List<Thread> consumerThreads = null;
	private Producer<HttpGet> producer = null;
	
	/**
	 * 
	 * @param numThreads
	 */
	public HttpGetRequestRunner(int numThreads) {
		this.numThreads = numThreads;
		this.httpGetRequestors = new ArrayList<Consumer<HttpGet>>(numThreads + 1);
		this.consumerThreads = new ArrayList<Thread>(numThreads + 1);
	}

	protected abstract Producer<HttpGet> createProducer() throws FillException;
	
	public int getNumThreads() { 
		return this.numThreads;
	}
	
	protected static Properties getConfig(String fileName) throws IOException {
		String surrogateFile = System.getProperty("configFile");
		if (null != surrogateFile && !"".equals(surrogateFile)) { 
			if (new File(surrogateFile).exists()) { 
				fileName = surrogateFile;
			}
		}
		Properties config = new Properties();
                System.err.println("loading config from file " + fileName);
		//config.load(getClass().getClassLoadeer().getResourceAsStream(fileName));
                config.load(ClassLoader.getSystemClassLoader().getSystemResourceAsStream(fileName));
		return config;
	}

	protected void registerShutdownHook(Thread t) { 
		Runtime.getRuntime().addShutdownHook(t);
	}

	@Override
	public synchronized void run() { 
	    try {
	    	
	    	producer = createProducer();
	
	    	createAndStartConsumers(producer);
	    	
	    	producer.start();
		    
	    } catch (Throwable t) { 
	    	logger.fatal("exception during execution, waiting for clean shutdown", t);
	    } finally { 
	    	shutdown();
	    }
    }
	
	private void createAndStartConsumers(Producer<HttpGet> producer) { 
		int numThreads = getNumThreads();
		
    	logger.info("Creating " + numThreads + " worker threads");

    	int i = 0;
    	for (; i < numThreads; i++) {
    		Consumer<HttpGet> httpGetRequestor = new HttpGetConsumer(producer);
    		httpGetRequestors.add(httpGetRequestor);
    		
    		Thread consumerThread = new Thread(httpGetRequestor);
    		consumerThreads.add(consumerThread);
		    	
    		consumerThread.start();
    	}

    	logger.info(i + " worker threads created");
	}
	
	protected synchronized void shutdown() { 

    	if (null != producer) { 
    		
    		/*
    		 * Complete processing of existing work queue
    		 */
    		boolean interrupted = Thread.interrupted(); // clears interrupt status

    		try { 
	    		logger.info("waiting for producer to exhaust log file sources; state: " + producer.getStateInfo());
	
	    		while (!producer.isExhausted()) {
			    	try { 
			    		Thread.sleep(5000);
			    	} catch (InterruptedException ie) { 
						logger.warn("runner interrupted waiting for log file source exhaustion", ie);
						interrupted = true;
			    	}
			    }
	
	    		logger.info("interrupting consumer threads");
	    		
	    		for (Thread consumerThread : consumerThreads) { 
	    			consumerThread.interrupt(); // wake from sleep, signal any threads blocking on take() to shutdown
	    			while (null != consumerThread) { 
	    				try { 
	    					logger.info("reaping thread " + consumerThread.getName());
	    					consumerThread.join();
	    					logger.info("thread " + consumerThread.getName() + " exited");
	    					consumerThread = null;
	    				} catch (InterruptedException ie) { 
	    					logger.warn("runner interrupted reaping child threads", ie);
				    		interrupted = true;
	    				}
	    			}
	    		}
	    		
	    		logger.info("printing consumer stats and shutting down");
	    		
	    		long runStartTime = Long.MAX_VALUE;
	    		long runEndTime = 0;
	    		long totalRequests = 0;
	
	    		for (Consumer<?> r : httpGetRequestors) { 
	    			r.printStats();
	    			if (r.getRunEndTime() > runEndTime) { 
	    				runEndTime = r.getRunEndTime();
	    			}
	
	    			if (r.getRunStartTime() < runStartTime) { 
	    				runStartTime = r.getRunStartTime();
	    			}
	    			totalRequests += r.getTotalRequests();
	
	    			r.shutdown();
	    		}
	
	    		if (runEndTime > 0) { 
	    			logger.info("================================Request Totals==================================");
	    			logger.info("Requests: " + totalRequests);
	    			logger.info("Run Time: " + (runEndTime - runStartTime) + " (ms) (" + new Date(runStartTime) + ", " + new Date(runEndTime) + ")");
	    		}
	    		
	    		producer = null;
    		
    		} finally { 
	    		if (interrupted) { 
	    			Thread.currentThread().interrupt();
	    		}	
    		}
    	}
	}
}
