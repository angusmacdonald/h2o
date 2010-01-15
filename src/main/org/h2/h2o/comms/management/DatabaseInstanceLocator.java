package org.h2.h2o.comms.management;

import java.rmi.AccessException;
import java.rmi.AlreadyBoundException;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.sql.SQLException;
import java.util.Set;

import org.h2.h2o.comms.DatabaseInstance;
import org.h2.h2o.comms.remote.DatabaseInstanceRemote;
import org.h2.h2o.util.DatabaseURL;

/**
 * @author Angus Macdonald (angus@cs.st-andrews.ac.uk)
 */
public class DatabaseInstanceLocator implements IDatabaseInstanceLocator {

	public static final String LOCAL_DATABASE_INSTANCE = "LOCAL_DB_INSTANCE";

	/*
	 * TODO will this class maintain a set of all database instances (requiring references to
	 * multiple registrys), or will it just maintain a reference to its own proxy?
	 */
	private Registry registry;
	
	private DatabaseInstanceRemote localInstance;
	
	/**
	 * 
	 * @param registry
	 * @param localInstance
	 */
	public DatabaseInstanceLocator(Registry registry, DatabaseInstanceRemote localInstance) {
		this.registry = registry;
		this.localInstance = localInstance;
		
		DatabaseInstanceRemote stub = null;
		
		try {
			stub = (DatabaseInstanceRemote) UnicastRemoteObject.exportObject(localInstance, 0);
		} catch (RemoteException e) {
			e.printStackTrace();
		}

		
		try {
			this.registry.bind(LOCAL_DATABASE_INSTANCE, stub);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}


	/* (non-Javadoc)
	 * @see org.h2.h2o.comms.management.IDatabaseInstanceLocator#removeLocalInstance()
	 */
	@Override
	public void removeLocalInstance() throws NotBoundException, RemoteException {
		try {
			registry.unbind(LOCAL_DATABASE_INSTANCE);
		} catch (AccessException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (RemoteException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}	

}
