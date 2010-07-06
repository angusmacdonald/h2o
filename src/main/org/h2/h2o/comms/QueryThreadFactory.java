package org.h2.h2o.comms;

import java.util.concurrent.ThreadFactory;

class QueryThreadFactory implements ThreadFactory {
	   public Thread newThread(Runnable r) {
	     return new Thread(r);
	   }
	 }
