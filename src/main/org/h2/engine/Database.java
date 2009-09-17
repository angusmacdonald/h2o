/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.engine;

import java.io.IOException;
import java.rmi.NotBoundException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Properties;
import java.util.Set;
import java.util.StringTokenizer;

import org.h2.api.DatabaseEventListener;
import org.h2.command.Command;
import org.h2.command.Parser;
import org.h2.command.dml.SetTypes;
import org.h2.constant.ErrorCode;
import org.h2.constant.LocationPreference;
import org.h2.constant.SysProperties;
import org.h2.constraint.Constraint;
import org.h2.h2o.comms.DataManagerLocator;
import org.h2.h2o.comms.DatabaseInstance;
import org.h2.h2o.comms.DatabaseInstanceLocator;
import org.h2.h2o.comms.DatabaseInstanceRemote;
import org.h2.h2o.comms.DatabaseURL;
import org.h2.h2o.comms.DataManagerRemote;
import org.h2.index.Cursor;
import org.h2.index.Index;
import org.h2.index.IndexType;
import org.h2.log.LogSystem;
import org.h2.log.UndoLogRecord;
import org.h2.message.Message;
import org.h2.message.Trace;
import org.h2.message.TraceSystem;
import org.h2.result.LocalResult;
import org.h2.result.Row;
import org.h2.result.SearchRow;
import org.h2.schema.Schema;
import org.h2.schema.SchemaObject;
import org.h2.schema.Sequence;
import org.h2.schema.TriggerObject;
import org.h2.store.DataHandler;
import org.h2.store.DataPage;
import org.h2.store.DiskFile;
import org.h2.store.FileLock;
import org.h2.store.FileStore;
import org.h2.store.PageStore;
import org.h2.store.RecordReader;
import org.h2.store.Storage;
import org.h2.store.WriterThread;
import org.h2.store.fs.FileSystem;
import org.h2.table.Column;
import org.h2.table.IndexColumn;
import org.h2.table.MetaTable;
import org.h2.table.ReplicaSet;
import org.h2.table.Table;
import org.h2.table.TableData;
import org.h2.table.TableLinkConnection;
import org.h2.table.TableView;
import org.h2.tools.DeleteDbFiles;
import org.h2.tools.Server;
import org.h2.util.BitField;
import org.h2.util.ByteUtils;
import org.h2.util.CacheLRU;
import org.h2.util.ClassUtils;
import org.h2.util.FileUtils;
import org.h2.util.IntHashMap;
import org.h2.util.NetUtils;
import org.h2.util.ObjectArray;
import org.h2.util.ObjectUtils;
import org.h2.util.SmallLRUCache;
import org.h2.util.StringUtils;
import org.h2.util.TempFileDeleter;
import org.h2.value.CompareMode;
import org.h2.value.Value;
import org.h2.value.ValueInt;
import org.h2.value.ValueLob;

import uk.ac.stand.dcs.nds.util.Diagnostic;

/**
 * There is one database object per open database.
 *
 * The format of the meta data table is:
 *  id int, headPos int (for indexes), objectType int, sql varchar
 *
 * @since 2004-04-15 22:49
 */
public class Database implements DataHandler {

	private static int initialPowerOffCount;

	private final boolean persistent;

	/**
	 * H2O. Indicates whether this database instance is managing the table schema for other running H2O instances.
	 */
	private boolean isSchemaManager;

	/**
	 * Is this instance connected to the schema manager?
	 */
	private boolean connectedToSM = false;

	/**
	 * H2O. The location of the schema manager in this system.
	 */
	private String schemaManagerLocation;

	/**
	 * H2O. The hostname/IP address on which this instance's TCP/FTP server is being run on.
	 */
	private String localMachineAddress;

	/**
	 * H2O. The port number on which this instance's TCP/FTP server is listening on.
	 */
	private int localMachinePort;

	/**
	 * H2O. The database location as specified in the JDBC connection string.
	 */
	private final String databaseLocation;

	/**
	 * H2O. Utility class for schema manager interactions.
	 */
	private SchemaManager schemaManager;

	/**
	 * Manages access to data managers, both local and remote.
	 */
	private DataManagerLocator dataManagerLocator;

	/**
	 * Manages access to remote database instances via RMI.
	 */
	private DatabaseInstanceLocator databaseInstanceLocator;
	
	private final String databaseName;
	private final String databaseShortName;
	private final String databaseURL;
	private final String cipher;
	private final byte[] filePasswordHash;

	private final HashMap<String, DbObject> roles = new HashMap<String, DbObject>();
	private final HashMap<String, DbObject> users = new HashMap<String, DbObject>();
	private final HashMap<String, DbObject> settings = new HashMap<String, DbObject>();
	private final HashMap<String, DbObject> schemas = new HashMap<String, DbObject>();
	private final HashMap<String, DbObject> rights = new HashMap<String, DbObject>();
	private final HashMap<String, DbObject> functionAliases = new HashMap<String, DbObject>();
	private final HashMap<String, DbObject> userDataTypes = new HashMap<String, DbObject>();
	private final HashMap<String, DbObject> aggregates = new HashMap<String, DbObject>();
	private final HashMap<String, DbObject> comments = new HashMap<String, DbObject>();
	private IntHashMap tableMap = new IntHashMap();
	private final HashMap<Integer, DbObject> databaseObjects = new HashMap<Integer, DbObject>();

	private final Set<Session> userSessions = Collections.synchronizedSet(new HashSet<Session>());
	private Session exclusiveSession;
	private final BitField objectIds = new BitField();
	private final Object lobSyncObject = new Object();

	private Schema mainSchema;
	private Schema infoSchema;

	private int nextSessionId;
	private User systemUser;
	private Session systemSession;
	private TableData meta;
	private Index metaIdIndex;
	private FileLock lock;
	private LogSystem log;
	private WriterThread writer;
	private IntHashMap storageMap = new IntHashMap();
	private boolean starting;
	private DiskFile fileData, fileIndex;
	private TraceSystem traceSystem;
	private DataPage dummy;
	private int fileLockMethod;
	private Role publicRole;
	private long modificationDataId;
	private long modificationMetaId;
	private CompareMode compareMode;
	private String cluster = Constants.CLUSTERING_DISABLED;
	private boolean readOnly;
	private boolean noDiskSpace;
	private int writeDelay = Constants.DEFAULT_WRITE_DELAY;
	private DatabaseEventListener eventListener;
	private int maxMemoryRows = Constants.DEFAULT_MAX_MEMORY_ROWS;
	private int maxMemoryUndo = SysProperties.DEFAULT_MAX_MEMORY_UNDO;
	private int lockMode = SysProperties.DEFAULT_LOCK_MODE;
	private boolean logIndexChanges;
	private int logLevel = 1;
	private int maxLengthInplaceLob = Constants.DEFAULT_MAX_LENGTH_INPLACE_LOB;
	private int allowLiterals = Constants.DEFAULT_ALLOW_LITERALS;

	private int powerOffCount = initialPowerOffCount;
	private int closeDelay;
	private DatabaseCloser delayedCloser;
	private boolean recovery;
	private volatile boolean closing;
	private boolean ignoreCase;
	private boolean deleteFilesOnDisconnect;
	private String lobCompressionAlgorithm;
	private boolean optimizeReuseResults = true;
	private String cacheType;
	private boolean indexSummaryValid = true;
	private String accessModeLog, accessModeData;
	private boolean referentialIntegrity = true;
	private boolean multiVersion;
	private DatabaseCloser closeOnExit;
	private Mode mode = Mode.getInstance(Mode.REGULAR);
	// TODO change in version 1.2
	private boolean multiThreaded;
	private int maxOperationMemory = SysProperties.DEFAULT_MAX_OPERATION_MEMORY;
	private boolean lobFilesInDirectories = SysProperties.LOB_FILES_IN_DIRECTORIES;
	private SmallLRUCache lobFileListCache = new SmallLRUCache(128);
	private boolean autoServerMode;
	private Server server;
	private HashMap linkConnections;
	private TempFileDeleter tempFileDeleter = TempFileDeleter.getInstance();
	private PageStore pageStore;

	private Properties reconnectLastLock;
	private long reconnectCheckNext;
	private boolean reconnectChangePending;

	/**
	 * H2O. Interface to this database instance, exposed via RMI.
	 */
	private DatabaseInstance databaseInstance;

	private String originalURL;


	public Database(String name, ConnectionInfo ci, String cipher) throws SQLException {
		
		this.compareMode = new CompareMode(null, null, 0);
		this.databaseLocation = ci.getSmallName();


		this.localMachineAddress = NetUtils.getLocalAddress();
		this.localMachinePort = ci.getPort();
		this.persistent = ci.isPersistent();
		this.isSchemaManager = ci.isSchemaManager();
		this.connectedToSM = this.isSchemaManager; //if it is the schema manager, it is connected.


		this.filePasswordHash = ci.getFilePasswordHash();
		this.databaseName = name;
		this.databaseShortName = parseDatabaseShortName();

		if (Constants.IS_H2O && !isManagementDB()) Diagnostic.traceNoEvent(Diagnostic.FINAL, "H2O, Database '" + name + "'.");
		
		this.schemaManagerLocation = ci.getSchemaManagerLocation();

		if (Constants.IS_H2O && !isManagementDB() && this.schemaManagerLocation == null){
			//throw new SQLException ("Schema Manager location should be known.");
			this.schemaManagerLocation = Constants.DEFAULT_SCHEMA_MANAGER_LOCATION;
		}

		if (Constants.IS_H2O && !isManagementDB()){ //don't run this code with the TCP server management DB
			DatabaseURL dbURL = DatabaseURL.parseURL(schemaManagerLocation);

			/* 
			 * Instantiate connections to RMI registry (if schema manager, create RMI registry first).
			 */
			int rmiPort = dbURL.getPort()+1;
			if (isSchemaManager){
				
				databaseInstanceLocator = new DatabaseInstanceLocator(rmiPort);
			} else {
				//Registry is located on a remote machine.
				databaseInstanceLocator = new DatabaseInstanceLocator(dbURL.getHostname(), rmiPort);
			}
		
			dataManagerLocator = new DataManagerLocator(dbURL.getHostname(), rmiPort);
		
		}
		
		this.multiThreaded = true; //H2O. Required for the H2O push replication feature, among other things.

		this.cipher = cipher;
		String lockMethodName = ci.getProperty("FILE_LOCK", null);
		this.accessModeLog = ci.getProperty("ACCESS_MODE_LOG", "rw").toLowerCase();
		this.accessModeData = ci.getProperty("ACCESS_MODE_DATA", "rw").toLowerCase();
		this.autoServerMode = ci.getProperty("AUTO_SERVER", false);
		if ("r".equals(accessModeData)) {
			readOnly = true;
			accessModeLog = "r";
		}
		this.fileLockMethod = FileLock.getFileLockMethod(lockMethodName);
		if (fileLockMethod == FileLock.LOCK_SERIALIZED) {
			writeDelay = SysProperties.MIN_WRITE_DELAY;
		}
		this.databaseURL = ci.getURL();
		this.originalURL = ci.getOriginalURL();
		this.eventListener = ci.getDatabaseEventListenerObject();
		ci.removeDatabaseEventListenerObject();
		if (eventListener == null) {
			String listener = ci.removeProperty("DATABASE_EVENT_LISTENER", null);
			if (listener != null) {
				listener = StringUtils.trim(listener, true, true, "'");
				setEventListenerClass(listener);
			}
		}
		String log = ci.getProperty(SetTypes.LOG, null);
		if (log != null) {
			this.logIndexChanges = "2".equals(log);
		}
		String ignoreSummary = ci.getProperty("RECOVER", null);
		if (ignoreSummary != null) {
			this.recovery = true;
		}
		this.multiVersion = ci.getProperty("MVCC", false);
		boolean closeAtVmShutdown = ci.getProperty("DB_CLOSE_ON_EXIT", true);
		int traceLevelFile = ci.getIntProperty(SetTypes.TRACE_LEVEL_FILE, TraceSystem.DEFAULT_TRACE_LEVEL_FILE);
		int traceLevelSystemOut = ci.getIntProperty(SetTypes.TRACE_LEVEL_SYSTEM_OUT,
				TraceSystem.DEFAULT_TRACE_LEVEL_SYSTEM_OUT);
		this.cacheType = StringUtils.toUpperEnglish(ci.removeProperty("CACHE_TYPE", CacheLRU.TYPE_NAME));
		openDatabase(traceLevelFile, traceLevelSystemOut, closeAtVmShutdown);
		if (Constants.IS_H2O && !isManagementDB()) Diagnostic.traceNoEvent(Diagnostic.FINAL, " Completed startup.");
	}

	private void openDatabase(int traceLevelFile, int traceLevelSystemOut, boolean closeAtVmShutdown) throws SQLException {
		try {
			open(traceLevelFile, traceLevelSystemOut);
			if (closeAtVmShutdown) {
				closeOnExit = new DatabaseCloser(this, 0, true);
				try {
					Runtime.getRuntime().addShutdownHook(closeOnExit);
				} catch (IllegalStateException e) {
					// shutdown in progress - just don't register the handler
					// (maybe an application wants to write something into a
					// database at shutdown time)
				} catch (SecurityException  e) {
					// applets may not do that - ignore
				}
			}
		} catch (Throwable e) {
			if (traceSystem != null) {
				if (e instanceof SQLException) {
					SQLException e2 = (SQLException) e;
					if (e2.getErrorCode() != ErrorCode.DATABASE_ALREADY_OPEN_1) {
						// only write if the database is not already in use
						traceSystem.getTrace(Trace.DATABASE).error("opening " + databaseName, e);
					}
				}
				traceSystem.close();
			}
			closeOpenFilesAndUnlock();
			throw Message.convert(e);
		}
	}

	public static void setInitialPowerOffCount(int count) {
		initialPowerOffCount = count;
	}

	public void setPowerOffCount(int count) {
		if (powerOffCount == -1) {
			return;
		}
		powerOffCount = count;
	}

	/**
	 * Check if two values are equal with the current comparison mode.
	 *
	 * @param a the first value
	 * @param b the second value
	 * @return true if both objects are equal
	 */
	public boolean areEqual(Value a, Value b) throws SQLException {
		// TODO optimization possible
		// boolean is = a.compareEqual(b);
		// boolean is2 = a.compareTo(b, compareMode) == 0;
		// if(is != is2) {
		// is = a.compareEqual(b);
		// System.out.println("hey!");
		// }
		// return a.compareEqual(b);
		return a.compareTo(b, compareMode) == 0;
	}

	/**
	 * Compare two values with the current comparison mode. The values may not
	 * be of the same type.
	 *
	 * @param a the first value
	 * @param b the second value
	 * @return 0 if both values are equal, -1 if the first value is smaller, and
	 *         1 otherwise
	 */
	public int compare(Value a, Value b) throws SQLException {
		return a.compareTo(b, compareMode);
	}

	/**
	 * Compare two values with the current comparison mode. The values must be
	 * of the same type.
	 *
	 * @param a the first value
	 * @param b the second value
	 * @return 0 if both values are equal, -1 if the first value is smaller, and
	 *         1 otherwise
	 */
	public int compareTypeSave(Value a, Value b) throws SQLException {
		return a.compareTypeSave(b, compareMode);
	}

	public long getModificationDataId() {
		return modificationDataId;
	}

	private void reconnectModified(boolean pending) {
		if (readOnly || pending == reconnectChangePending || lock == null) {
			return;
		}
		try {
			if (pending) {
				getTrace().debug("wait before writing");
				Thread.sleep((int) (SysProperties.RECONNECT_CHECK_DELAY * 1.1));
			}
			lock.setProperty("modificationDataId", Long.toString(modificationDataId));
			lock.setProperty("modificationMetaId", Long.toString(modificationMetaId));
			lock.setProperty("changePending", pending ? "true" : null);
			lock.save();
			reconnectLastLock = lock.load();
			reconnectChangePending = pending;
		} catch (Exception e) {
			getTrace().error("pending:"+ pending, e);
		}
	}

	public long getNextModificationDataId() {
		return ++modificationDataId;
	}

	public long getModificationMetaId() {
		return modificationMetaId;
	}

	public long getNextModificationMetaId() {
		// if the meta data has been modified, the data is modified as well
		// (because MetaTable returns modificationDataId)
		modificationDataId++;
		return modificationMetaId++;
	}

	public int getPowerOffCount() {
		return powerOffCount;
	}

	public void checkPowerOff() throws SQLException {
		if (powerOffCount == 0) {
			return;
		}
		if (powerOffCount > 1) {
			powerOffCount--;
			return;
		}
		if (powerOffCount != -1) {
			try {
				powerOffCount = -1;
				if (log != null) {
					try {
						stopWriter();
						log.close();
					} catch (SQLException e) {
						// ignore
					}
					log = null;
				}
				if (fileData != null) {
					try {
						fileData.close();
					} catch (SQLException e) {
						// ignore
					}
					fileData = null;
				}
				if (fileIndex != null) {
					try {
						fileIndex.close();
					} catch (SQLException e) {
						// ignore
					}
					fileIndex = null;
				}
				if (pageStore != null) {
					try {
						pageStore.close();
					} catch (SQLException e) {
						// ignore
					}
					pageStore = null;
				}
				if (lock != null) {
					stopServer();
					if (fileLockMethod != FileLock.LOCK_SERIALIZED) {
						// allow testing shutdown
						lock.unlock();
					}
					lock = null;
				}
			} catch (Exception e) {
				TraceSystem.traceThrowable(e);
			}
		}
		Engine.getInstance().close(databaseName);
		throw Message.getSQLException(ErrorCode.SIMULATED_POWER_OFF);
	}

	/**
	 * Check if a database with the given name exists.
	 *
	 * @param name the name of the database (including path)
	 * @return true if one exists
	 */
	public static boolean exists(String name) {
		return FileUtils.exists(name + Constants.SUFFIX_DATA_FILE);
	}

	/**
	 * Get the trace object for the given module.
	 *
	 * @param module the module name
	 * @return the trace object
	 */
	public Trace getTrace(String module) {
		return traceSystem.getTrace(module);
	}

	public FileStore openFile(String name, String mode, boolean mustExist) throws SQLException {
		if (mustExist && !FileUtils.exists(name)) {
			throw Message.getSQLException(ErrorCode.FILE_NOT_FOUND_1, name);
		}
		FileStore store = FileStore.open(this, name, mode, cipher, filePasswordHash);
		try {
			store.init();
		} catch (SQLException e) {
			store.closeSilently();
			throw e;
		}
		return store;
	}

	/**
	 * Check if the file password hash is correct.
	 *
	 * @param cipher the cipher algorithm
	 * @param hash the hash code
	 * @return true if the cipher algorithm and the password match
	 */
	public boolean validateFilePasswordHash(String cipher, byte[] hash) throws SQLException {
		if (!StringUtils.equals(cipher, this.cipher)) {
			return false;
		}
		return ByteUtils.compareSecure(hash, filePasswordHash);
	}

	private void openFileData() throws SQLException {
		fileData = new DiskFile(this, databaseName + Constants.SUFFIX_DATA_FILE, accessModeData, true, true,
				SysProperties.CACHE_SIZE_DEFAULT);
	}

	private void openFileIndex() throws SQLException {
		fileIndex = new DiskFile(this, databaseName + Constants.SUFFIX_INDEX_FILE, accessModeData, false,
				logIndexChanges, SysProperties.CACHE_SIZE_INDEX_DEFAULT);
	}

	public DataPage getDataPage() {
		return dummy;
	}

	private String parseDatabaseShortName() {
		String n = databaseName;
		if (n.endsWith(":")) {
			n = null;
		}
		if (n != null) {
			StringTokenizer tokenizer = new StringTokenizer(n, "/\\:,;");
			while (tokenizer.hasMoreTokens()) {
				n = tokenizer.nextToken();
			}
		}
		if (n == null || n.length() == 0) {
			n = "UNNAMED";
		}
		return StringUtils.toUpperEnglish(n);
	}

	private synchronized void open(int traceLevelFile, int traceLevelSystemOut) throws SQLException {
		boolean databaseExists = false; //whether the database already exists on disk. i.e. with .db.data files, etc.

		if (persistent) {

			if (SysProperties.PAGE_STORE) {
				String pageFileName = databaseName + Constants.SUFFIX_PAGE_FILE;
				if (FileUtils.exists(pageFileName) && FileUtils.isReadOnly(pageFileName)) {
					readOnly = true;
				}
			}
			String dataFileName = databaseName + Constants.SUFFIX_DATA_FILE;

			databaseExists = FileUtils.exists(dataFileName);

			if (FileUtils.exists(dataFileName)) {
				// if it is already read-only because ACCESS_MODE_DATA=r
				readOnly = readOnly | FileUtils.isReadOnly(dataFileName);
			}
			if (readOnly) {
				traceSystem = new TraceSystem(null, false);
			} else {
				traceSystem = new TraceSystem(databaseName + Constants.SUFFIX_TRACE_FILE, true);
			}
			traceSystem.setLevelFile(traceLevelFile);
			traceSystem.setLevelSystemOut(traceLevelSystemOut);
			traceSystem.getTrace(Trace.DATABASE)
			.info("opening " + databaseName + " (build " + Constants.BUILD_ID + ")");
			if (autoServerMode) {
				if (readOnly || fileLockMethod == FileLock.LOCK_NO) {
					throw Message.getSQLException(ErrorCode.FEATURE_NOT_SUPPORTED);
				}
			}
			if (!readOnly && fileLockMethod != FileLock.LOCK_NO) {
				lock = new FileLock(traceSystem, databaseName + Constants.SUFFIX_LOCK_FILE, Constants.LOCK_SLEEP);
				lock.lock(fileLockMethod);
				if (autoServerMode) {
					startServer(lock.getUniqueId());
				}
			}
			// wait until pending changes are written
			isReconnectNeeded();
			if (SysProperties.PAGE_STORE) {
				PageStore store = getPageStore();
				store.recover();
			}
			if (FileUtils.exists(dataFileName)) {
				lobFilesInDirectories &= !ValueLob.existsLobFile(getDatabasePath());
				lobFilesInDirectories |= FileUtils.exists(databaseName + Constants.SUFFIX_LOBS_DIRECTORY);
			}
			dummy = DataPage.create(this, 0);
			deleteOldTempFiles();
			log = new LogSystem(this, databaseName, readOnly, accessModeLog, pageStore);
			if (pageStore == null) {
				openFileData();
				log.open();
				openFileIndex();
				log.recover();
				fileData.init();
				try {
					fileIndex.init();
				} catch (Exception e) {
					if (recovery) {
						traceSystem.getTrace(Trace.DATABASE).error("opening index", e);
						ArrayList<DbObject> list = new ArrayList<DbObject>(storageMap.values());
						for (int i = 0; i < list.size(); i++) {
							Storage s = (Storage) list.get(i);
							if (s.getDiskFile() == fileIndex) {
								removeStorage(s.getId(), fileIndex);
							}
						}
						fileIndex.delete();
						openFileIndex();
					} else {
						throw Message.convert(e);
					}
				}
			}
			reserveLobFileObjectIds();
			writer = WriterThread.create(this, writeDelay);
		} else {
			traceSystem = new TraceSystem(null, false);
			log = new LogSystem(null, null, false, null, null);
		}
		systemUser = new User(this, 0, Constants.DBA_NAME, true);
		mainSchema = new Schema(this, 0, Constants.SCHEMA_MAIN, systemUser, true);
		infoSchema = new Schema(this, -1, Constants.SCHEMA_INFORMATION, systemUser, true);

		schemas.put(mainSchema.getName(), mainSchema);
		schemas.put(infoSchema.getName(), infoSchema);
		publicRole = new Role(this, 0, Constants.PUBLIC_ROLE_NAME, true);
		roles.put(Constants.PUBLIC_ROLE_NAME, publicRole);
		systemUser.setAdmin(true);
		systemSession = new Session(this, systemUser, ++nextSessionId);

		ObjectArray cols = new ObjectArray();
		Column columnId = new Column("ID", Value.INT);
		columnId.setNullable(false);
		cols.add(columnId);
		cols.add(new Column("HEAD", Value.INT));
		cols.add(new Column("TYPE", Value.INT));
		cols.add(new Column("SQL", Value.STRING));
		int headPos = 0;
		if (pageStore != null) {
			headPos = pageStore.getMetaTableHeadPos();
		}
		meta = mainSchema.createTable("SYS", 0, cols, persistent, false, headPos);
		tableMap.put(0, meta);
		IndexColumn[] pkCols = IndexColumn.wrap(new Column[] { columnId });
		metaIdIndex = meta.addIndex(systemSession, "SYS_ID", 0, pkCols, IndexType.createPrimaryKey(
				false, false), Index.EMPTY_HEAD, null);
		objectIds.set(0);
		// there could be views on system tables, so they must be added first
		for (int i = 0; i < MetaTable.getMetaTableTypeCount(); i++) {
			addMetaData(i);
		}

		starting = true;

		if (Constants.IS_H2O && !isManagementDB()){ //don't run this code with the TCP server management DB
			/*
			 * Add this database instance to the RMI registry.
			 * This must be done before meta-records are executed.
			 */
			String dbURL = DatabaseURL.parseURL(this.originalURL).getNewURL();
			Diagnostic.traceNoEvent(Diagnostic.FULL, "Creating remote proxy for database instance: " + dbURL);
			databaseInstance = new DatabaseInstance(dbURL, systemSession); //original URL may contain 'localhost'.
			databaseInstanceLocator.registerDatabaseInstance(databaseInstance);
		}
		
		Cursor cursor = metaIdIndex.find(systemSession, null, null);
		// first, create all function aliases and sequences because
		// they might be used in create table / view / constraints and so on

		ObjectArray records = new ObjectArray();

		while (cursor.next()) {
			MetaRecord rec = new MetaRecord(cursor.get());
			objectIds.set(rec.getId());
			records.add(rec);
		}

		MetaRecord.sort(records);

		for (int i = 0; i < records.size(); i++) {

			MetaRecord rec = (MetaRecord) records.get(i);

			rec.execute(this, systemSession, eventListener);
		}
		if (Constants.IS_H2O && !isManagementDB()) Diagnostic.traceNoEvent(Diagnostic.FINAL, " Executed meta-records.");

		// try to recompile the views that are invalid
		recompileInvalidViews(systemSession);

		starting = false;
		addDefaultSetting(systemSession, SetTypes.DEFAULT_LOCK_TIMEOUT, null, Constants.INITIAL_LOCK_TIMEOUT);
		addDefaultSetting(systemSession, SetTypes.DEFAULT_TABLE_TYPE, null, Constants.DEFAULT_TABLE_TYPE);
		addDefaultSetting(systemSession, SetTypes.CACHE_SIZE, null, SysProperties.CACHE_SIZE_DEFAULT);
		addDefaultSetting(systemSession, SetTypes.CLUSTER, Constants.CLUSTERING_DISABLED, 0);
		addDefaultSetting(systemSession, SetTypes.WRITE_DELAY, null, Constants.DEFAULT_WRITE_DELAY);
		addDefaultSetting(systemSession, SetTypes.CREATE_BUILD, null, Constants.BUILD_ID);
		if (!readOnly) {
			removeUnusedStorages(systemSession);
		}

		systemSession.commit(true);
		traceSystem.getTrace(Trace.DATABASE).info("opened " + databaseName);

		if (Constants.IS_H2O && !isManagementDB() && ( !databaseExists || !isSchemaManager)){ //don't run this code with the TCP server management DB

			createH2OTables();

			Diagnostic.traceNoEvent(Diagnostic.FINAL, " Created schema manager tables.");
		} 

	}

	public Schema getMainSchema() {
		return mainSchema;
	}

	private void startServer(String key) throws SQLException {
		server = Server.createTcpServer(new String[]{
				"-tcpPort", "0",
				"-tcpAllowOthers", "true",
				"-key", key, databaseName});
		server.start();
		String address = NetUtils.getLocalAddress() + ":" + server.getPort();
		Diagnostic.traceNoEvent(Diagnostic.FINAL, "Server started on: " + address);
		lock.setProperty("server", address);
		lock.save();
	}

	private void stopServer() {
		if (server != null) {
			Server s = server;
			// avoid calling stop recursively
			// because stopping the server will
			// try to close the database as well
			server = null;
			s.stop();
		}
	}

	private void recompileInvalidViews(Session session) {
		boolean recompileSuccessful;
		do {
			recompileSuccessful = false;


			Set<Table> alltables = getAllReplicas();

			for (Table table: alltables) {
				if (table instanceof TableView) {
					TableView view = (TableView) table;
					if (view.getInvalid()) {
						try {
							view.recompile(session);
						} catch (SQLException e) {
							// ignore
						}
						if (!view.getInvalid()) {
							recompileSuccessful = true;
						}
					}
				}
			}
		} while (recompileSuccessful);
		// when opening a database, views are initialized before indexes,
		// so they may not have the optimal plan yet
		// this is not a problem, it is just nice to see the newest plan

		Set<Table> allTables = getAllReplicas();

		for (Table table: allTables) {
			if (table instanceof TableView) {
				TableView view = (TableView) table;
				if (!view.getInvalid()) {
					try {
						view.recompile(systemSession);
					} catch (SQLException e) {
						// ignore
					}
				}
			}
		}
	}

	private void removeUnusedStorages(Session session) throws SQLException {
		if (persistent) {
			ObjectArray storages = getAllStorages();
			for (int i = 0; i < storages.size(); i++) {
				Storage storage = (Storage) storages.get(i);
				if (storage != null && storage.getRecordReader() == null) {
					storage.truncate(session);
				}
			}
		}
	}

	private void addDefaultSetting(Session session, int type, String stringValue, int intValue) throws SQLException {
		if (readOnly) {
			return;
		}
		String name = SetTypes.getTypeName(type);
		if (settings.get(name) == null) {
			Setting setting = new Setting(this, allocateObjectId(false, true), name);
			if (stringValue == null) {
				setting.setIntValue(intValue);
			} else {
				setting.setStringValue(stringValue);
			}
			addDatabaseObject(session, setting);
		}
	}

	/**
	 * Remove the storage object from the file.
	 *
	 * @param id the storage id
	 * @param file the file
	 */
	public void removeStorage(int id, DiskFile file) {
		if (SysProperties.CHECK) {
			Storage s = (Storage) storageMap.get(id);
			if (s == null || s.getDiskFile() != file) {
				Message.throwInternalError();
			}
		}
		storageMap.remove(id);
	}

	/**
	 * Get the storage object for the given file. An new object is created if
	 * required.
	 *
	 * @param id the storage id
	 * @param file the file
	 * @return the storage object
	 */
	public Storage getStorage(int id, DiskFile file) {
		Storage storage = (Storage) storageMap.get(id);
		if (storage != null) {
			if (SysProperties.CHECK && storage.getDiskFile() != file) {
				Message.throwInternalError();
			}
		} else {
			storage = new Storage(this, file, null, id);
			storageMap.put(id, storage);
		}
		return storage;
	}

	private void addMetaData(int type) throws SQLException {
		MetaTable m = new MetaTable(infoSchema, -1 - type, type);
		infoSchema.add(m);
	}

	private synchronized void addMeta(Session session, DbObject obj) throws SQLException {
		int id = obj.getId();
		if (id > 0 && !starting && !obj.getTemporary()) {
			Row r = meta.getTemplateRow();
			MetaRecord rec = new MetaRecord(obj);
			rec.setRecord(r);
			objectIds.set(id);
			meta.lock(session, true, true);
			meta.addRow(session, r);
			if (isMultiVersion()) {
				// TODO this should work without MVCC, but avoid risks at the moment
				session.log(meta, UndoLogRecord.INSERT, r);
			}
		}
		if (SysProperties.PAGE_STORE && id > 0) {
			databaseObjects.put(ObjectUtils.getInteger(id), obj);
		}
	}

	/**
	 * Remove the given object from the meta data.
	 *
	 * @param session the session
	 * @param id the id of the object to remove
	 */
	public synchronized void removeMeta(Session session, int id) throws SQLException {
		if (id > 0 && !starting) {
			SearchRow r = meta.getTemplateSimpleRow(false);
			r.setValue(0, ValueInt.get(id));
			Cursor cursor = metaIdIndex.find(session, r, r);
			if (cursor.next()) {
				Row found = cursor.get();
				meta.lock(session, true, true);
				meta.removeRow(session, found);
				if (isMultiVersion()) {
					// TODO this should work without MVCC, but avoid risks at the
					// moment
					session.log(meta, UndoLogRecord.DELETE, found);
				}
				objectIds.clear(id);
				if (SysProperties.CHECK) {
					checkMetaFree(session, id);
				}
			}
		}
		if (SysProperties.PAGE_STORE) {
			databaseObjects.remove(ObjectUtils.getInteger(id));
		}
	}

	private HashMap<String, DbObject> getMap(int type) {
		switch (type) {
		case DbObject.USER:
			return users;
		case DbObject.SETTING:
			return settings;
		case DbObject.ROLE:
			return roles;
		case DbObject.RIGHT:
			return rights;
		case DbObject.FUNCTION_ALIAS:
			return functionAliases;
		case DbObject.SCHEMA:
			return schemas;
		case DbObject.USER_DATATYPE:
			return userDataTypes;
		case DbObject.COMMENT:
			return comments;
		case DbObject.AGGREGATE:
			return aggregates;
		default:
			throw Message.throwInternalError("type=" + type);
		}
	}

	/**
	 * Add a schema object to the database.
	 *
	 * @param session the session
	 * @param obj the object to add
	 */
	public synchronized void addSchemaObject(Session session, SchemaObject obj) throws SQLException {
		int id = obj.getId();
		if (id > 0 && !starting) {
			checkWritingAllowed();
		}
		obj.getSchema().add(obj);
		addMeta(session, obj);
		if (obj instanceof TableData) {
			tableMap.put(id, obj);
		}
	}

	/**
	 * Add an object to the database.
	 *
	 * @param session the session
	 * @param obj the object to add
	 */
	public synchronized void addDatabaseObject(Session session, DbObject obj) throws SQLException {
		int id = obj.getId();
		if (id > 0 && !starting) {
			checkWritingAllowed();
		}
		HashMap<String, DbObject> map = getMap(obj.getType());
		if (obj.getType() == DbObject.USER) {
			User user = (User) obj;
			if (user.getAdmin() && systemUser.getName().equals(Constants.DBA_NAME)) {
				systemUser.rename(user.getName());
			}
		}
		String name = obj.getName();
		if (SysProperties.CHECK && map.get(name) != null) {
			Message.throwInternalError("object already exists");
		}
		addMeta(session, obj);
		map.put(name, obj);
	}

	/**
	 * Get the user defined aggregate function if it exists, or null if not.
	 *
	 * @param name the name of the user defined aggregate function
	 * @return the aggregate function or null
	 */
	public UserAggregate findAggregate(String name) {
		return (UserAggregate) aggregates.get(name);
	}

	/**
	 * Get the comment for the given database object if one exists, or null if
	 * not.
	 *
	 * @param object the database object
	 * @return the comment or null
	 */
	public Comment findComment(DbObject object) {
		if (object.getType() == DbObject.COMMENT) {
			return null;
		}
		String key = Comment.getKey(object);
		return (Comment) comments.get(key);
	}

	/**
	 * Get the user defined function if it exists, or null if not.
	 *
	 * @param name the name of the user defined function
	 * @return the function or null
	 */
	public FunctionAlias findFunctionAlias(String name) {
		return (FunctionAlias) functionAliases.get(name);
	}

	/**
	 * Get the role if it exists, or null if not.
	 *
	 * @param roleName the name of the role
	 * @return the role or null
	 */
	public Role findRole(String roleName) {
		return (Role) roles.get(roleName);
	}

	/**
	 * Get the schema if it exists, or null if not.
	 *
	 * @param schemaName the name of the schema
	 * @return the schema or null
	 */
	public Schema findSchema(String schemaName) {
		return (Schema) schemas.get(schemaName);
	}

	/**
	 * Get the setting if it exists, or null if not.
	 *
	 * @param name the name of the setting
	 * @return the setting or null
	 */
	public Setting findSetting(String name) {
		return (Setting) settings.get(name);
	}

	/**
	 * Get the user if it exists, or null if not.
	 *
	 * @param name the name of the user
	 * @return the user or null
	 */
	public User findUser(String name) {
		return (User) users.get(name);
	}

	/**
	 * Get the user defined data type if it exists, or null if not.
	 *
	 * @param name the name of the user defined data type
	 * @return the user defined data type or null
	 */
	public UserDataType findUserDataType(String name) {
		return (UserDataType) userDataTypes.get(name);
	}

	/**
	 * Get user with the given name. This method throws an exception if the user
	 * does not exist.
	 *
	 * @param name the user name
	 * @return the user
	 * @throws SQLException if the user does not exist
	 */
	public User getUser(String name) throws SQLException {
		User user = findUser(name);
		//H2O bug fix (where admin user is not found on startup queries
		if (user == null){
			user = systemUser;
		}
		if (user == null) {
			throw Message.getSQLException(ErrorCode.USER_NOT_FOUND_1, name);
		}
		return user;
	}

	/**
	 * Create a session for the given user.
	 *
	 * @param user the user
	 * @return the session
	 * @throws SQLException if the database is in exclusive mode
	 */
	public synchronized Session createSession(User user) throws SQLException {
		if (exclusiveSession != null) {
			throw Message.getSQLException(ErrorCode.DATABASE_IS_IN_EXCLUSIVE_MODE);
		}
		Session session = new Session(this, user, ++nextSessionId);
		userSessions.add(session);
		traceSystem.getTrace(Trace.SESSION).info("connecting #" + session.getId() + " to " + databaseName);
		if (delayedCloser != null) {
			delayedCloser.reset();
			delayedCloser = null;
		}
		return session;
	}

	/**
	 * Remove a session. This method is called after the user has disconnected.
	 *
	 * @param session the session
	 */
	public synchronized void removeSession(Session session) {
		if (session != null) {
			if (exclusiveSession == session) {
				exclusiveSession = null;
			}
			userSessions.remove(session);
			if (session != systemSession) {
				traceSystem.getTrace(Trace.SESSION).info("disconnecting #" + session.getId());
			}
		}
		if (userSessions.size() == 0 && session != systemSession) {
			if (closeDelay == 0) {
				close(false);
			} else if (closeDelay < 0) {
				return;
			} else {
				delayedCloser = new DatabaseCloser(this, closeDelay * 1000, false);
				delayedCloser.setName("H2 Close Delay " + getShortName());
				delayedCloser.setDaemon(true);
				delayedCloser.start();
			}
		}
		if (session != systemSession && session != null) {
			traceSystem.getTrace(Trace.SESSION).info("disconnected #" + session.getId());
		}
	}

	/**
	 * Close the database.
	 *
	 * @param fromShutdownHook true if this method is called from the shutdown
	 *            hook
	 */
	public synchronized void close(boolean fromShutdownHook) {
		
		if (closing) {
			return;
		}

		Diagnostic.traceNoEvent(Diagnostic.FULL, DatabaseURL.parseURL(this.originalURL).getNewURL());
		
		closing = true;
		stopServer();

		if (Constants.IS_H2O && !isManagementDB() && !fromShutdownHook){
			try {
				databaseInstanceLocator.removeRegistryObject(databaseInstance.getName(), false);
			} catch (NotBoundException e) {
				/*
				 * Not a big problem because all we are doing at this stage is trying to remove it.
				 * Happens nearly every time the database is closed from the shutdown hook, hence being inside this IF statement.
				 */
				Diagnostic.traceNoEvent(Diagnostic.FULL, "Attempted to remove database instance from registry, but it wasn't found.");
			}
			databaseInstanceLocator = null;
		}

		if (userSessions.size() > 0) {
			if (!fromShutdownHook) {
				return;
			}
			traceSystem.getTrace(Trace.DATABASE).info("closing " + databaseName + " from shutdown hook");
			Session[] all = new Session[userSessions.size()];
			userSessions.toArray(all);
			for (int i = 0; i < all.length; i++) {
				Session s = all[i];
				try {
					// must roll back, otherwise the session is removed and
					// the log file that contains its uncommitted operations as well
					s.rollback();
					s.close();
				} catch (SQLException e) {
					traceSystem.getTrace(Trace.SESSION).error("disconnecting #" + s.getId(), e);
				}
			}
		}
		if (log != null) {
			log.setDisabled(false);
		}
		traceSystem.getTrace(Trace.DATABASE).info("closing " + databaseName);
		if (eventListener != null) {
			// allow the event listener to connect to the database
			closing = false;
			DatabaseEventListener e = eventListener;
			// set it to null, to make sure it's called only once
			eventListener = null;
			e.closingDatabase();
			if (userSessions.size() > 0) {
				// if a connection was opened, we can't close the database
				return;
			}
			closing = true;
		}
		try {
			if (systemSession != null) {
				Set<Table> alltables = getAllReplicas();

				for (Table table: alltables) {
					table.close(systemSession);
				}
				ObjectArray sequences = getAllSchemaObjects(DbObject.SEQUENCE);
				for (int i = 0; i < sequences.size(); i++) {
					Sequence sequence = (Sequence) sequences.get(i);
					sequence.close();
				}
				ObjectArray triggers = getAllSchemaObjects(DbObject.TRIGGER);
				for (int i = 0; i < triggers.size(); i++) {
					TriggerObject trigger = (TriggerObject) triggers.get(i);
					trigger.close();
				}
				meta.close(systemSession);
				systemSession.commit(true);
				indexSummaryValid = true;
			}
		} catch (SQLException e) {
			traceSystem.getTrace(Trace.DATABASE).error("close", e);
		}
		// remove all session variables
		if (persistent) {
			try {
				ValueLob.removeAllForTable(this, ValueLob.TABLE_ID_SESSION_VARIABLE);
			} catch (SQLException e) {
				traceSystem.getTrace(Trace.DATABASE).error("close", e);
			}
		}
		tempFileDeleter.deleteAll();
		try {
			closeOpenFilesAndUnlock();
		} catch (SQLException e) {
			traceSystem.getTrace(Trace.DATABASE).error("close", e);
		}
		traceSystem.getTrace(Trace.DATABASE).info("closed");
		traceSystem.close();
		if (closeOnExit != null) {
			closeOnExit.reset();
			try {
				Runtime.getRuntime().removeShutdownHook(closeOnExit);
			} catch (IllegalStateException e) {
				// ignore
			} catch (SecurityException  e) {
				// applets may not do that - ignore
			}
			closeOnExit = null;
		}
		Engine.getInstance().close(databaseName);
		if (deleteFilesOnDisconnect && persistent) {
			deleteFilesOnDisconnect = false;
			try {
				String directory = FileUtils.getParent(databaseName);
				String name = FileUtils.getFileName(databaseName);
				DeleteDbFiles.execute(directory, name, true);
			} catch (Exception e) {
				// ignore (the trace is closed already)
			}
		}
		closing = false;
	}

	private void stopWriter() {
		if (writer != null) {
			try {
				writer.stopThread();
			} catch (SQLException e) {
				traceSystem.getTrace(Trace.DATABASE).error("close", e);
			}
			writer = null;
		}
	}

	private synchronized void closeOpenFilesAndUnlock() throws SQLException {
		if (log != null) {
			stopWriter();
			try {
				log.close();
			} catch (Throwable e) {
				traceSystem.getTrace(Trace.DATABASE).error("close", e);
			}
			log = null;
		}
		if (pageStore != null) {
			pageStore.checkpoint();
		}
		closeFiles();
		if (persistent && lock == null && fileLockMethod != FileLock.LOCK_NO) {
			// everything already closed (maybe in checkPowerOff)
			// don't delete temp files in this case because
			// the database could be open now (even from within another process)
			return;
		}
		if (persistent) {
			deleteOldTempFiles();
		}
		if (systemSession != null) {
			systemSession.close();
			systemSession = null;
		}
		if (lock != null) {
			lock.unlock();
			lock = null;
		}
	}

	private void closeFiles() {
		try {
			if (fileData != null) {
				fileData.close();
				fileData = null;
			}
			if (fileIndex != null) {
				fileIndex.close();
				fileIndex = null;
			}
			if (pageStore != null) {
				pageStore.close();
				pageStore = null;
			}
		} catch (SQLException e) {
			traceSystem.getTrace(Trace.DATABASE).error("close", e);
		}
		storageMap.clear();
	}

	private void checkMetaFree(Session session, int id) throws SQLException {
		SearchRow r = meta.getTemplateSimpleRow(false);
		r.setValue(0, ValueInt.get(id));
		Cursor cursor = metaIdIndex.find(session, r, r);
		if (cursor.next()) {
			Message.throwInternalError();
		}
	}

	public synchronized int allocateObjectId(boolean needFresh, boolean dataFile) {
		// TODO refactor: use hash map instead of bit field for object ids
		needFresh = true;
		int i;
		if (needFresh) {
			i = objectIds.getLastSetBit() + 1;
			if ((i & 1) != (dataFile ? 1 : 0)) {
				i++;
			}

			while (storageMap.get(i) != null || objectIds.get(i)) {
				i++;
				if ((i & 1) != (dataFile ? 1 : 0)) {
					i++;
				}
			}
		} else {
			i = objectIds.nextClearBit(0);
		}
		if (SysProperties.CHECK && objectIds.get(i)) {
			Message.throwInternalError();
		}
		objectIds.set(i);
		return i;
	}

	public ObjectArray getAllAggregates() {
		return new ObjectArray(aggregates.values());
	}

	public ObjectArray getAllComments() {
		return new ObjectArray(comments.values());
	}

	public ObjectArray getAllFunctionAliases() {
		return new ObjectArray(functionAliases.values());
	}

	public int getAllowLiterals() {
		if (starting) {
			return Constants.ALLOW_LITERALS_ALL;
		}
		return allowLiterals;
	}

	public ObjectArray getAllRights() {
		return new ObjectArray(rights.values());
	}

	public ObjectArray getAllRoles() {
		return new ObjectArray(roles.values());
	}

	/**
	 * Get all schema objects of the given type.
	 *
	 * @param type the object type
	 * @return all objects of that type
	 */
	public ObjectArray getAllSchemaObjects(int type) {
		ObjectArray list = new ObjectArray();
		for (Iterator<DbObject> it = schemas.values().iterator(); it.hasNext();) {
			Schema schema = (Schema) it.next();
			list.addAll(schema.getAll(type));
		}
		return list;
	}

	/**
	 * Get all tables. Replaces the getAllSchemaObjects method for this particular call.
	 *
	 * @param type the object type
	 * @return all objects of that type
	 */
	public Set<ReplicaSet> getAllTables() {
		Set<ReplicaSet> list = new HashSet<ReplicaSet>();
		for (Iterator<DbObject> it = schemas.values().iterator(); it.hasNext();) {
			Schema schema = (Schema) it.next();
			list.addAll(schema.getTablesAndViews().values());
		}
		return list;
	}

	/**
	 * Get every single table instance, including replicas for the same table.
	 * @return
	 */
	public Set<Table> getAllReplicas(){
		Set<ReplicaSet> allReplicaSets = getAllTables();

		Set<Table> alltables = new HashSet<Table>();
		for (ReplicaSet tableSet: allReplicaSets){
			alltables.addAll(tableSet.getAllCopies());
		}

		return alltables;
	}

	public ObjectArray getAllSchemas() {
		return new ObjectArray(schemas.values());
	}

	public ObjectArray getAllSettings() {
		return new ObjectArray(settings.values());
	}

	public ObjectArray getAllStorages() {
		return new ObjectArray(storageMap.values());
	}

	public ObjectArray getAllUserDataTypes() {
		return new ObjectArray(userDataTypes.values());
	}

	public ObjectArray getAllUsers() {
		return new ObjectArray(users.values());
	}

	public String getCacheType() {
		return cacheType;
	}

	public int getChecksum(byte[] data, int start, int end) {
		int x = 0;
		while (start < end) {
			x += data[start++];
		}
		return x;
	}

	public String getCluster() {
		return cluster;
	}

	public CompareMode getCompareMode() {
		return compareMode;
	}

	public String getDatabasePath() {
		if (persistent) {
			return FileUtils.getAbsolutePath(databaseName);
		}
		return null;
	}

	public String getShortName() {
		return databaseShortName;
	}

	public String getName() {
		return databaseName;
	}

	public LogSystem getLog() {
		return log;
	}

	/**
	 * Get all sessions that are currently connected to the database.
	 *
	 * @param includingSystemSession if the system session should also be
	 *            included
	 * @return the list of sessions
	 */
	public Session[] getSessions(boolean includingSystemSession) {
		ArrayList<Session> list = new ArrayList<Session>(userSessions);
		if (includingSystemSession && systemSession != null) {
			list.add(systemSession);
		}
		Session[] array = new Session[list.size()];
		list.toArray(array);
		return array;
	}

	/**
	 * Update an object in the system table.
	 *
	 * @param session the session
	 * @param obj the database object
	 */
	public synchronized void update(Session session, DbObject obj) throws SQLException {
		int id = obj.getId();
		removeMeta(session, id);
		addMeta(session, obj);
	}

	/**
	 * Rename a schema object.
	 *
	 * @param session the session
	 * @param obj the object
	 * @param newName the new name
	 */
	public synchronized void renameSchemaObject(Session session, SchemaObject obj, String newName) throws SQLException {
		checkWritingAllowed();
		obj.getSchema().rename(obj, newName);
		updateWithChildren(session, obj);
	}

	private synchronized void updateWithChildren(Session session, DbObject obj) throws SQLException {
		ObjectArray list = obj.getChildren();
		Comment comment = findComment(obj);
		if (comment != null) {
			Message.throwInternalError();
		}
		update(session, obj);
		// remember that this scans only one level deep!
		for (int i = 0; list != null && i < list.size(); i++) {
			DbObject o = (DbObject) list.get(i);
			if (o.getCreateSQL() != null) {
				update(session, o);
			}
		}
	}

	/**
	 * Rename a database object.
	 *
	 * @param session the session
	 * @param obj the object
	 * @param newName the new name
	 */
	public synchronized void renameDatabaseObject(Session session, DbObject obj, String newName) throws SQLException {
		checkWritingAllowed();
		int type = obj.getType();
		HashMap<String, DbObject> map = getMap(type);
		if (SysProperties.CHECK) {
			if (!map.containsKey(obj.getName())) {
				Message.throwInternalError("not found: " + obj.getName());
			}
			if (obj.getName().equals(newName) || map.containsKey(newName)) {
				Message.throwInternalError("object already exists: " + newName);
			}
		}
		obj.checkRename();
		int id = obj.getId();
		removeMeta(session, id);
		map.remove(obj.getName());
		obj.rename(newName);
		map.put(newName, obj);
		updateWithChildren(session, obj);
	}

	public String createTempFile() throws SQLException {
		try {
			boolean inTempDir = readOnly;
			String name = databaseName;
			if (!persistent) {
				name = FileSystem.PREFIX_MEMORY + name;
			}
			return FileUtils.createTempFile(name, Constants.SUFFIX_TEMP_FILE, true, inTempDir);
		} catch (IOException e) {
			throw Message.convertIOException(e, databaseName);
		}
	}

	private void reserveLobFileObjectIds() throws SQLException {
		String prefix = FileUtils.normalize(databaseName) + ".";
		String path = FileUtils.getParent(databaseName);
		String[] list = FileUtils.listFiles(path);
		for (int i = 0; i < list.length; i++) {
			String name = list[i];
			if (name.endsWith(Constants.SUFFIX_LOB_FILE) && FileUtils.fileStartsWith(name, prefix)) {
				name = name.substring(prefix.length());
				name = name.substring(0, name.length() - Constants.SUFFIX_LOB_FILE.length());
				int dot = name.indexOf('.');
				if (dot >= 0) {
					String id = name.substring(dot + 1);
					int objectId = Integer.parseInt(id);
					objectIds.set(objectId);
				}
			}
		}
	}

	private void deleteOldTempFiles() throws SQLException {
		String path = FileUtils.getParent(databaseName);
		String prefix = FileUtils.normalize(databaseName);
		String[] list = FileUtils.listFiles(path);
		for (int i = 0; i < list.length; i++) {
			String name = list[i];
			if (name.endsWith(Constants.SUFFIX_TEMP_FILE) && FileUtils.fileStartsWith(name, prefix)) {
				// can't always delete the files, they may still be open
				FileUtils.tryDelete(name);
			}
		}
	}

	/**
	 * Get or create the specified storage object.
	 *
	 * @param reader the record reader
	 * @param id the object id
	 * @param dataFile true if the data is in the data file
	 * @return the storage
	 */
	public Storage getStorage(RecordReader reader, int id, boolean dataFile) {
		DiskFile file;
		if (dataFile) {
			file = fileData;
		} else {
			file = fileIndex;
		}
		Storage storage = getStorage(id, file);
		storage.setReader(reader);
		return storage;
	}

	/**
	 * Get the schema. If the schema does not exist, an exception is thrown.
	 *
	 * @param schemaName the name of the schema
	 * @return the schema
	 * @throws SQLException no schema with that name exists
	 */
	public Schema getSchema(String schemaName) throws SQLException {
		Schema schema = findSchema(schemaName);
		if (schema == null) {
			throw Message.getSQLException(ErrorCode.SCHEMA_NOT_FOUND_1, schemaName);
		}
		return schema;
	}

	/**
	 * Remove the object from the database.
	 *
	 * @param session the session
	 * @param obj the object to remove
	 */
	public synchronized void removeDatabaseObject(Session session, DbObject obj) throws SQLException {
		checkWritingAllowed();
		String objName = obj.getName();
		int type = obj.getType();
		HashMap<String, DbObject> map = getMap(type);
		if (SysProperties.CHECK && !map.containsKey(objName)) {
			Message.throwInternalError("not found: " + objName);
		}
		Comment comment = findComment(obj);
		if (comment != null) {
			removeDatabaseObject(session, comment);
		}
		int id = obj.getId();
		obj.removeChildrenAndResources(session);
		map.remove(objName);
		removeMeta(session, id);
	}

	/**
	 * Get the first table that depends on this object.
	 *
	 * @param obj the object to find
	 * @param except the table to exclude (or null)
	 * @return the first dependent table, or null
	 */
	public ReplicaSet getDependentTable(SchemaObject obj, Table except) {
		switch (obj.getType()) {
		case DbObject.COMMENT:
		case DbObject.CONSTRAINT:
		case DbObject.INDEX:
		case DbObject.RIGHT:
		case DbObject.TRIGGER:
		case DbObject.USER:
			return null;
		default:
		}
		Set<ReplicaSet> list = getAllTables();
		Set set = new HashSet();

		Set<ReplicaSet> allreplicas = getAllTables();

		for (ReplicaSet replicaSet: allreplicas) {


			if ((except != null) && (replicaSet.getACopy() != null) && except.getName().equalsIgnoreCase(replicaSet.getACopy().getName())) {
				continue;
			}

			set.clear();
			replicaSet.addDependencies(set);
			if (set.contains(obj)) {
				return replicaSet;
			}
		}
		return null;
	}

	private String getFirstInvalidTable(Session session) {
		String conflict = null;
		try {
			Set<ReplicaSet> list = getAllTables();
			for (ReplicaSet replicaSet: list) {

				conflict = replicaSet.getSQL();
				session.prepare(replicaSet.getCreateSQL());
			}
		} catch (SQLException e) {
			return conflict;
		}
		return null;
	}

	/**
	 * Remove an object from the system table.
	 *
	 * @param session the session
	 * @param obj the object to be removed
	 */
	public synchronized void removeSchemaObject(Session session, SchemaObject obj) throws SQLException {
		int type = obj.getType();
		if (type == DbObject.TABLE_OR_VIEW) {
			Table table = (Table) obj;
			if (table.getTemporary() && !table.getGlobalTemporary()) {
				session.removeLocalTempTable(table);
				return;
			}
		} else if (type == DbObject.INDEX) {
			Index index = (Index) obj;
			Table table = index.getTable();
			if (table.getTemporary() && !table.getGlobalTemporary()) {
				session.removeLocalTempTableIndex(index);
				return;
			}
		} else if (type == DbObject.CONSTRAINT) {
			Constraint constraint = (Constraint) obj;
			Table table = constraint.getTable();
			if (table.getTemporary() && !table.getGlobalTemporary()) {
				session.removeLocalTempTableConstraint(constraint);
				return;
			}
		}
		checkWritingAllowed();
		Comment comment = findComment(obj);
		if (comment != null) {
			removeDatabaseObject(session, comment);
		}
		obj.getSchema().remove(obj);
		if (!starting) {
			String invalid;
			if (SysProperties.OPTIMIZE_DROP_DEPENDENCIES) {
				ReplicaSet replicaSet = getDependentTable(obj, null);
				invalid = replicaSet == null ? null : replicaSet.getSQL();
			} else {
				invalid = getFirstInvalidTable(session);
			}
			if (invalid != null) {
				obj.getSchema().add(obj);
				throw Message.getSQLException(ErrorCode.CANNOT_DROP_2, new String[] { obj.getSQL(), invalid });
			}
			obj.removeChildrenAndResources(session);
		}
		int id = obj.getId();
		removeMeta(session, id);
		if (obj instanceof TableData) {
			tableMap.remove(id);
		}
	}

	/**
	 * Check if this database disk-based.
	 *
	 * @return true if it is disk-based, false it it is in-memory only.
	 */
	public boolean isPersistent() {
		return persistent;
	}

	public TraceSystem getTraceSystem() {
		return traceSystem;
	}

	public DiskFile getDataFile() {
		return fileData;
	}

	public DiskFile getIndexFile() {
		return fileIndex;
	}

	public synchronized void setCacheSize(int kb) throws SQLException {
		if (fileData != null) {
			fileData.getCache().setMaxSize(kb);
			int valueIndex = kb <= 32 ? kb : (kb >>> SysProperties.CACHE_SIZE_INDEX_SHIFT);
			fileIndex.getCache().setMaxSize(valueIndex);
		}
	}

	public synchronized void setMasterUser(User user) throws SQLException {
		addDatabaseObject(systemSession, user);
		systemSession.commit(true);
	}

	public Role getPublicRole() {
		return publicRole;
	}

	/**
	 * Get a unique temporary table name.
	 *
	 * @param sessionId the session id
	 * @return a unique name
	 */
	public String getTempTableName(int sessionId) {
		String tempName;
		for (int i = 0;; i++) {
			tempName = Constants.TEMP_TABLE_PREFIX + sessionId + "_" + i;
			if (mainSchema.findTableOrView(null, tempName, LocationPreference.NO_PREFERENCE) == null) {
				break;
			}
		}
		return tempName;
	}

	public void setCompareMode(CompareMode compareMode) {
		this.compareMode = compareMode;
	}

	public void setCluster(String cluster) {
		this.cluster = cluster;
	}

	public void checkWritingAllowed() throws SQLException {
		if (readOnly) {
			throw Message.getSQLException(ErrorCode.DATABASE_IS_READ_ONLY);
		}
		if (noDiskSpace) {
			throw Message.getSQLException(ErrorCode.NO_DISK_SPACE_AVAILABLE);
		}
	}

	public boolean getReadOnly() {
		return readOnly;
	}

	public void setWriteDelay(int value) {
		writeDelay = value;
		if (writer != null) {
			writer.setWriteDelay(value);
		}
	}

	/**
	 * Delete an unused log file. It is deleted immediately if no writer thread
	 * is running, or deleted later on if one is running. Deleting is delayed
	 * because the hard drive otherwise may delete the file a bit before the
	 * data is written to the new file, which can cause problems when
	 * recovering.
	 *
	 * @param fileName the name of the file to be deleted
	 */
	public void deleteLogFileLater(String fileName) throws SQLException {
		if (writer != null) {
			writer.deleteLogFileLater(fileName);
		} else {
			FileUtils.delete(fileName);
		}
	}

	public void setEventListener(DatabaseEventListener eventListener) {
		this.eventListener = eventListener;
	}

	public void setEventListenerClass(String className) throws SQLException {
		if (className == null || className.length() == 0) {
			eventListener = null;
		} else {
			try {
				eventListener = (DatabaseEventListener) ClassUtils.loadUserClass(className).newInstance();
				String url = databaseURL;
				if (cipher != null) {
					url += ";CIPHER=" + cipher;
				}
				eventListener.init(url);
			} catch (Throwable e) {
				throw Message.getSQLException(ErrorCode.ERROR_SETTING_DATABASE_EVENT_LISTENER_2, new String[] {
						className, e.toString() }, e);
			}
		}
	}

	public synchronized void freeUpDiskSpace() throws SQLException {
		if (eventListener != null) {
			eventListener.diskSpaceIsLow(0);
		}
	}

	/**
	 * Set the progress of a long running operation.
	 * This method calls the {@link DatabaseEventListener} if one is registered.
	 *
	 * @param state the {@link DatabaseEventListener} state
	 * @param name the object name
	 * @param x the current position
	 * @param max the highest value
	 */

	public void setProgress(int state, String name, int x, int max) {
		if (eventListener != null) {
			try {
				eventListener.setProgress(state, name, x, max);
			} catch (Exception e2) {
				// ignore this second (user made) exception
			}
		}
	}

	/**
	 * This method is called after an exception occurred, to inform the database
	 * event listener (if one is set).
	 *
	 * @param e the exception
	 * @param sql the SQL statement
	 */
	public void exceptionThrown(SQLException e, String sql) {
		if (eventListener != null) {
			try {
				eventListener.exceptionThrown(e, sql);
			} catch (Exception e2) {
				// ignore this second (user made) exception
			}
		}
	}

	/**
	 * Synchronize the files with the file system. This method is called when
	 * executing the SQL statement CHECKPOINT SYNC.
	 */
	public void sync() throws SQLException {
		if (log != null) {
			log.sync();
		}
		if (fileData != null) {
			fileData.sync();
		}
		if (fileIndex != null) {
			fileIndex.sync();
		}
	}

	public int getMaxMemoryRows() {
		return maxMemoryRows;
	}

	public void setMaxMemoryRows(int value) {
		this.maxMemoryRows = value;
	}

	public void setMaxMemoryUndo(int value) {
		this.maxMemoryUndo = value;
	}

	public int getMaxMemoryUndo() {
		return maxMemoryUndo;
	}

	public void setLockMode(int lockMode) throws SQLException {
		switch (lockMode) {
		case Constants.LOCK_MODE_OFF:
		case Constants.LOCK_MODE_READ_COMMITTED:
		case Constants.LOCK_MODE_TABLE:
		case Constants.LOCK_MODE_TABLE_GC:
			break;
		default:
			throw Message.getInvalidValueException("lock mode", "" + lockMode);
		}
		this.lockMode = lockMode;
	}

	public int getLockMode() {
		return lockMode;
	}

	public synchronized void setCloseDelay(int value) {
		this.closeDelay = value;
	}

	public boolean getLogIndexChanges() {
		return logIndexChanges;
	}

	public synchronized void setLog(int level) throws SQLException {
		if (logLevel == level) {
			return;
		}
		boolean logData;
		boolean logIndex;
		switch (level) {
		case 0:
			logData = false;
			logIndex = false;
			break;
		case 1:
			logData = true;
			logIndex = false;
			break;
		case 2:
			logData = true;
			logIndex = true;
			break;
		default:
			throw Message.throwInternalError("level=" + level);
		}
		if (fileIndex != null) {
			fileIndex.setLogChanges(logIndex);
		}
		logIndexChanges = logIndex;
		if (log != null) {
			log.setDisabled(!logData);
			log.checkpoint();
		}
		traceSystem.getTrace(Trace.DATABASE).error("SET LOG " + level, null);
		logLevel = level;
	}

	public boolean getRecovery() {
		return recovery;
	}

	public Session getSystemSession() {
		return systemSession;
	}

	public void handleInvalidChecksum() throws SQLException {
		SQLException e = Message.getSQLException(ErrorCode.FILE_CORRUPTED_1, "wrong checksum");
		if (!recovery) {
			throw e;
		}
		traceSystem.getTrace(Trace.DATABASE).error("recover", e);
	}

	/**
	 * Check if the database is in the process of closing.
	 *
	 * @return true if the database is closing
	 */
	public boolean isClosing() {
		return closing;
	}

	public void setMaxLengthInplaceLob(int value) {
		this.maxLengthInplaceLob = value;
	}

	public int getMaxLengthInplaceLob() {
		return persistent ? maxLengthInplaceLob : Integer.MAX_VALUE;
	}

	public void setIgnoreCase(boolean b) {
		ignoreCase = b;
	}

	public boolean getIgnoreCase() {
		if (starting) {
			// tables created at startup must not be converted to ignorecase
			return false;
		}
		return ignoreCase;
	}

	public synchronized void setDeleteFilesOnDisconnect(boolean b) {
		this.deleteFilesOnDisconnect = b;
	}

	public String getLobCompressionAlgorithm(int type) {
		return lobCompressionAlgorithm;
	}

	public void setLobCompressionAlgorithm(String stringValue) {
		this.lobCompressionAlgorithm = stringValue;
	}

	/**
	 * Called when the size if the data or index file has been changed.
	 *
	 * @param length the new file size
	 */
	public void notifyFileSize(long length) {
		// ignore
	}

	public synchronized void setMaxLogSize(long value) {
		getLog().setMaxLogSize(value);
	}

	public void setAllowLiterals(int value) {
		this.allowLiterals = value;
	}

	public boolean getOptimizeReuseResults() {
		return optimizeReuseResults;
	}

	public void setOptimizeReuseResults(boolean b) {
		optimizeReuseResults = b;
	}

	/**
	 * Called when the summary of the index in the log file has become invalid.
	 * This method is only called if index changes are not logged, and if an
	 * index has been changed.
	 */
	public void invalidateIndexSummary() throws SQLException {
		if (indexSummaryValid) {
			indexSummaryValid = false;
			
			if (log == null){
				log = new LogSystem(this, databaseName, readOnly, accessModeLog, pageStore);
			}
			log.invalidateIndexSummary();
		}
	}

	public boolean getIndexSummaryValid() {
		return indexSummaryValid;
	}

	public Object getLobSyncObject() {
		return lobSyncObject;
	}

	public int getSessionCount() {
		return userSessions.size();
	}

	public void setReferentialIntegrity(boolean b) {
		referentialIntegrity = b;
	}

	public boolean getReferentialIntegrity() {
		return referentialIntegrity;
	}

	/**
	 * Check if the database is currently opening. This is true until all stored
	 * SQL statements have been executed.
	 *
	 * @return true if the database is still starting
	 */
	public boolean isStarting() {
		return starting;
	}

	/**
	 * Check if multi version concurrency is enabled for this database.
	 *
	 * @return true if it is enabled
	 */
	public boolean isMultiVersion() {
		return multiVersion;
	}

	/**
	 * Called after the database has been opened and initialized. This method
	 * notifies the event listener if one has been set.
	 */
	public void opened() {
		if (eventListener != null) {
			eventListener.opened();
		}
	}

	public void setMode(Mode mode) {
		this.mode = mode;
	}

	public Mode getMode() {
		return mode;
	}

	public boolean isMultiThreaded() {
		return multiThreaded;
	}

	public void setMultiThreaded(boolean multiThreaded) throws SQLException {
		if (multiThreaded && multiVersion && this.multiThreaded != multiThreaded) {
			// currently the combination of MVCC and MULTI_THREADED is not supported
			throw Message.getSQLException(ErrorCode.CANNOT_CHANGE_SETTING_WHEN_OPEN_1, "MVCC & MULTI_THREADED");
		}
		this.multiThreaded = multiThreaded;
	}

	public void setMaxOperationMemory(int maxOperationMemory) {
		this.maxOperationMemory  = maxOperationMemory;
	}

	public int getMaxOperationMemory() {
		return maxOperationMemory;
	}

	public Session getExclusiveSession() {
		return exclusiveSession;
	}

	public void setExclusiveSession(Session session) {
		this.exclusiveSession = session;
	}

	public boolean getLobFilesInDirectories() {
		return lobFilesInDirectories;
	}

	public SmallLRUCache getLobFileListCache() {
		return lobFileListCache;
	}

	/**
	 * Checks if the system table (containing the catalog) is locked.
	 *
	 * @return true if it is currently locked
	 */
	public boolean isSysTableLocked() {
		return meta.isLockedExclusively();
	}

	/**
	 * Open a new connection or get an existing connection to another database.
	 *
	 * @param driver the database driver or null
	 * @param url the database URL
	 * @param user the user name
	 * @param password the password
	 * @return the connection
	 */
	public TableLinkConnection getLinkConnection(String driver, String url, String user, String password) throws SQLException {
		if (linkConnections == null) {
			linkConnections = new HashMap();
		}
		return TableLinkConnection.open(linkConnections, driver, url, user, password);
	}

	public String toString() {
		return databaseShortName + ":" + super.toString();
	}

	/**
	 * Immediately close the database.
	 */
	public void shutdownImmediately() {
		setPowerOffCount(1);
		try {
			checkPowerOff();
		} catch (SQLException e) {
			// ignore
		}
	}

	public TempFileDeleter getTempFileDeleter() {
		return tempFileDeleter;
	}

	public Trace getTrace() {
		return getTrace(Trace.DATABASE);
	}

	public PageStore getPageStore() throws SQLException {
		if (pageStore == null && SysProperties.PAGE_STORE) {
			pageStore = new PageStore(this, databaseName + Constants.SUFFIX_PAGE_FILE, accessModeData,
					SysProperties.CACHE_SIZE_DEFAULT);
			pageStore.open();
		}
		return pageStore;
	}

	/**
	 * Redo a change in a table.
	 *
	 * @param tableId the object id of the table
	 * @param row the row
	 * @param add true if the record is added, false if deleted
	 */
	public void redo(int tableId, Row row, boolean add) throws SQLException {
		TableData table = (TableData) tableMap.get(tableId);
		if (add) {
			table.addRow(systemSession, row);
		} else {
			table.removeRow(systemSession, row);
		}
		if (tableId == 0) {
			MetaRecord m = new MetaRecord(row);
			if (add) {
				objectIds.set(m.getId());
				m.execute(this, systemSession, eventListener);
			} else {
				m.undo(this, systemSession, eventListener);
			}
		}
	}

	/**
	 * Get the first user defined table.
	 *
	 * @return the table or null if no table is defined
	 */
	public ReplicaSet getFirstUserTable() {

		Set<ReplicaSet> list = getAllTables();
		for (ReplicaSet replicaSet: list) {

			if (replicaSet.getCreateSQL()!= null){
				return replicaSet;
			}
		}

		return null;
	}

	public boolean isReconnectNeeded() {
		if (fileLockMethod != FileLock.LOCK_SERIALIZED) {
			return false;
		}
		long now = System.currentTimeMillis();
		if (now < reconnectCheckNext) {
			return false;
		}
		reconnectCheckNext = now + SysProperties.RECONNECT_CHECK_DELAY;
		if (lock == null) {
			lock = new FileLock(traceSystem, databaseName + Constants.SUFFIX_LOCK_FILE, Constants.LOCK_SLEEP);
		}
		Properties prop;
		try {
			while (true) {
				prop = lock.load();
				if (prop.equals(reconnectLastLock)) {
					return false;
				}
				if (prop.getProperty("changePending", null) == null) {
					break;
				}
				getTrace().debug("delay (change pending)");
				Thread.sleep(SysProperties.RECONNECT_CHECK_DELAY);
			}
			reconnectLastLock = prop;
		} catch (Exception e) {
			getTrace().error("readOnly:" + readOnly, e);
			// ignore
		}
		return true;
	}

	/**
	 * This method is called after writing to the database.
	 */
	public void afterWriting() throws SQLException {
		if (fileLockMethod != FileLock.LOCK_SERIALIZED || readOnly) {
			return;
		}
		reconnectCheckNext = System.currentTimeMillis() + 1;
	}

	/**
	 * Flush all changes when using the serialized mode, and if there are
	 * pending changes.
	 */
	public void checkpointIfRequired() throws SQLException {
		if (fileLockMethod != FileLock.LOCK_SERIALIZED || readOnly || !reconnectChangePending) {
			return;
		}
		long now = System.currentTimeMillis();
		if (now > reconnectCheckNext) {
			getTrace().debug("checkpoint");
			checkpoint();
			reconnectModified(false);
		}
	}

	/**
	 * Flush all changes and open a new log file.
	 */
	public void checkpoint() throws SQLException {
		if (SysProperties.PAGE_STORE) {
			pageStore.checkpoint();
		}
		getLog().checkpoint();
		getTempFileDeleter().deleteUnused();
	}

	/**
	 * This method is called before writing to the log file.
	 */
	public void beforeWriting() {
		if (fileLockMethod == FileLock.LOCK_SERIALIZED) {
			reconnectModified(true);
		}
	}

	/**
	 * Get a database object.
	 *
	 * @param id the object id
	 * @return the database object
	 */
	DbObject getDbObject(int id) {
		return databaseObjects.get(ObjectUtils.getInteger(id));
	}

	/**
	 * H2O Creates H2O schema meta-data tables, including schema manager tables if this machine is a schema manager.
	 * @throws SQLException 
	 */
	private void createH2OTables(){
		schemaManager = SchemaManager.getInstance(systemSession); //H2O. Create schema manager utility class.

		int result = -1;

		try {
			if (isSchemaManager){ // Create the schema manager tables and immediately add local tables to this manager.

				result = schemaManager.createSchemaManagerTables();

			} else { // Not a schema manager -  Create a linked table connection to the remote schema manager

				result = schemaManager.createLinkedTablesForSchemaManager(schemaManagerLocation);

			}

			try { 
				DataManager.createDataManagerTables(systemSession);
			} catch (SQLException e) {

				e.printStackTrace();
			}

			if (!schemaManager.connectionInformationExists(localMachineAddress, localMachinePort)){
				schemaManager.addLocalConnectionInformation(localMachineAddress, localMachinePort, getDatabaseLocation());
			}

		} catch (SQLException e) {
			e.printStackTrace();
		}


		if (!isSchemaManager && result >= 0){
			connectedToSM = true;

			try {

				LocalResult remoteTables = schemaManager.getAllRemoteTables(localMachineAddress, localMachinePort, getDatabaseLocation());

				String sql = "";
				while (remoteTables.next()){
					Value[] row = remoteTables.currentRow();
					String schemaName = row[0].getString();
					String tableName = row[1].getString();
					String db_location = row[2].getString();
					String connection_type = row[3].getString();
					String machine_name = row[4].getString();
					String connection_port = row[5].getString();

					//Example format: jdbc:h2:sm:tcp://localhost:9090/db_data/one/test_db

					String fullTableName = schemaName + "." + tableName;
					String dbname = "";

					if (connection_type.equals("tcp")){
						dbname = "jdbc:h2:" + connection_type + "://" + machine_name + ":" + connection_port + "/" + db_location;
					} else if (connection_type.equals("mem")){
						dbname = "jdbc:h2:" + db_location;
					} else {
						Message.throwInternalError("This connection type isn't supported yet. Get on that!");
					}
					sql += "\nCREATE LINKED TABLE IF NOT EXISTS " + fullTableName + "('org.h2.Driver', '" + dbname + "', '" + 
					SchemaManager.USERNAME + "', '" + SchemaManager.PASSWORD + "', '" + fullTableName + "');";

				}

				if (!sql.equals("")){
					Parser queryParser = new Parser(systemSession, true);
					Command sqlQuery = queryParser.prepareCommand(sql);
					sqlQuery.executeUpdate();
				}


			} catch (SQLException e) {
				connectedToSM = false;
				e.printStackTrace();
			}


		}


	}

	/**
	 * @return the connectedToSM
	 */
	public boolean isConnectedToSM() {
		return connectedToSM;
	}

	/**
	 * Is this database instance a schema manager?
	 * @return true if it is a schema manager.
	 */
	public boolean isSM() {
		return isSchemaManager;
	}

	/**
	 * @return the databaseLocation
	 */
	public String getDatabaseLocation() {
		return databaseLocation;
	}

	/**
	 * Is this database instance an H2 management database?
	 * @return
	 */
	public boolean isManagementDB() {
		return databaseShortName.startsWith("MANAGEMENT_DB_");
	}

	/**
	 * @return the localMachineAddress
	 */
	public String getLocalMachineAddress() {
		return localMachineAddress;
	}

	/**
	 * @return the localMachinePort
	 */
	public int getLocalMachinePort() {
		return localMachinePort;
	}

	/**
	 * Gets the full address of the database - i.e. one that can be used to connect to it
	 * remotely through JDBC. An example path:  jdbc:h2:sm:tcp://localhost:9090/db_data/one/test_db
	 * @return
	 */
	public String getFullDatabasePath() {
		String isTCP = (localMachinePort == -1 && databaseLocation.contains("mem"))? "": "tcp:";

		String url = "";
		if (isTCP.equals("tcp:")){
			url = getLocalMachineAddress() + ":" + getLocalMachinePort() + "/";
		}

		return "jdbc:h2:" + ((isSM())? "sm:": "") + isTCP + url + getDatabaseLocation();
	}

	/**
	 * Returns the type of connection this database is open on (e.g. tcp, mem).
	 * @return
	 */
	public String getConnectionType() {
		return (localMachinePort == -1 && databaseLocation.contains("mem"))? "mem": "tcp";
	}

	public void addDataManager(DataManager dm){
		dataManagerLocator.registerDataManager(dm);
	}

	public DataManagerRemote getDataManager(String tableName) throws SQLException{
		return dataManagerLocator.lookupDataManager(tableName);
	}

	/**
	 * @param string
	 */
	public void removeDataManager(String tableName, boolean removeLocalOnly) {
		dataManagerLocator.removeRegistryObject(tableName, removeLocalOnly);

	}

	/**
	 * @param replicaLocations
	 * @return
	 */
	public Set<DatabaseInstanceRemote> getDatabaseInstances(
			Set<String> replicaLocations) {
		return databaseInstanceLocator.getInstances(replicaLocations);
	}

	/**
	 * @param replicaLocationString
	 * @return
	 */
	public DatabaseInstanceRemote getDatabaseInstance(
			String replicaLocationString) {
		return databaseInstanceLocator.getInstance(replicaLocationString);
	}
	
	/**
	 * @return
	 */
	public DatabaseInstanceRemote getLocalDatabaseInstance() {
		return getDatabaseInstance(originalURL);
	}
	
	public String getOriginalDatabaseURL(){
		return originalURL;
	}
	
	public void removeLocalDatabaseInstance(){
		try {
			databaseInstanceLocator.removeRegistryObject(databaseInstance.getName(), false);
		} catch (NotBoundException e) {
			/*
			 * Not a big problem because all we are doing at this stage is trying to remove it.
			 * Happens nearly every time the database is closed from the shutdown hook, hence being inside this IF statement.
			 */
			Diagnostic.traceNoEvent(Diagnostic.FULL, "Attempted to remove database instance from registry, but it wasn't found.");
		}
		
	}


}
