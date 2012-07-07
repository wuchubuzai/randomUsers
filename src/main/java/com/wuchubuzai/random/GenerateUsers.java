package com.wuchubuzai.random;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;

import me.prettyprint.cassandra.serializers.LongSerializer;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.ddl.ColumnFamilyDefinition;
import me.prettyprint.hector.api.ddl.KeyspaceDefinition;
import me.prettyprint.hector.api.exceptions.HectorException;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.mutation.Mutator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Generate random users! 
 * @author sd
 *
 */
public class GenerateUsers {

	private static final Logger LOG = LoggerFactory.getLogger(GenerateUsers.class);
	
	// override via properties file:  GenerateUsers.properties
	private static String keyspace  = "random";
	private static String userRowKey = "users";
	private static String userCf = "user";
	private static String hostIp = "127.0.0.1:9160";
	private static String clusterName = "cluster";
	
	// create 25,000 user entries
	private static int numUsers = 1000;
	private static int numThreads = 100;
	
	private static Cluster cluster;
	private static Random generator = new Random();
	
	private static boolean singleTest = false;
	private static Map<Integer, String> firstNames = new HashMap<Integer, String>();
	private static Map<Integer, String> lastNames = new HashMap<Integer, String>();
	
	public static void main(String[] args) {

		String propertyFileName = "GenerateUsers.properties";
		
		Properties properties = new Properties();
		boolean propertiesExist = false;
		try {
			InputStream inputStream = GenerateUsers.class.getClassLoader().getResourceAsStream(propertyFileName);
			if (inputStream != null) {
				properties.load(inputStream);
				propertiesExist = true;
			}
		} catch (IOException e) {
			LOG.error("IOException: " + e.getMessage());
		}
		
		if (propertiesExist) { 
			if (LOG.isInfoEnabled()) {
				LOG.info("properties file found with " + properties.size()  + " properties detected");
			}

			// check for overrides for defaults
			String[] overrides = { RandomUserConstants.KEYSPACE_KEY, "userRowKey", "userCf", "hostIp", "clusterName", "numUsers", "numThreads", "singleTest" };
			for (int i = 0; i < overrides.length; i++) {
				if (properties.containsKey(overrides[i]) && overrides[i].equalsIgnoreCase("singletest")) {  
					if (LOG.isInfoEnabled()) {
						LOG.info("singleTest property detected");
					}
					if (properties.getProperty(overrides[i]).equalsIgnoreCase("true")) { 
						setNumUsers(1);
						setNumThreads(1);
						setSingleTest(true);
					}
				} else { 
					if (properties.containsKey(overrides[i])) { 
						if (LOG.isInfoEnabled()) {
							LOG.info(overrides[i] + " external property defined");
						}
						if (overrides[i].equals(RandomUserConstants.KEYSPACE_KEY)) {
							if (LOG.isInfoEnabled()) {
								LOG.info("override keyspace: " + properties.getProperty(overrides[i]));
							}
							setKeyspace(properties.getProperty(overrides[i]));
						}
						
						if (overrides[i].equals("userRowKey")) {
							if (LOG.isInfoEnabled()) {
								LOG.info("override userRowKey: " + properties.getProperty(overrides[i]));
							}
							setUserRowKey(properties.getProperty(overrides[i]));
						}
						
						if (overrides[i].equals("userCf")) {
							if (LOG.isInfoEnabled()) {
								LOG.info("override userCf: " + properties.getProperty(overrides[i]));
							}
							setUserCf(properties.getProperty(overrides[i]));
						}
						
						if (overrides[i].equals("hostIp")) {
							if (LOG.isInfoEnabled()) {
								LOG.info("override hostIp: " + properties.getProperty(overrides[i]));
							}
							setHostIp(properties.getProperty(overrides[i]));
						}
						
						if (overrides[i].equals("clusterName")) {
							if (LOG.isInfoEnabled()) {
								LOG.info("override clusterName: " + properties.getProperty(overrides[i]));
							}
							setClusterName(properties.getProperty(overrides[i]));
						}
						
						if (overrides[i].equals("numUsers")) {
							if (LOG.isInfoEnabled()) {
								LOG.info("override numUsers: " + properties.getProperty(overrides[i]));
							}
							setNumUsers(Integer.parseInt(properties.getProperty(overrides[i])));
						}
						
						if (overrides[i].equals("numThreads")) {
							if (LOG.isInfoEnabled()) {
								LOG.info("override numThreads: " + properties.getProperty(overrides[i]));
							}
							setNumThreads(Integer.parseInt(properties.getProperty(overrides[i])));
						}
					}
				}
			}
		}
		
		
		try {
			// load the csv to get the first names and last names
			File file = new File("etc/randomNames.csv");			
			BufferedReader br = new BufferedReader(new FileReader(file));
			String line = null;
			int i = 0;
			while ((line = br.readLine()) != null) {
				String tokens[] = line.split(",");
				firstNames.put(i, tokens[0]);
				lastNames.put(i, tokens[1]);				
				i++;
			}
		} catch (FileNotFoundException e) {
			LOG.error(e.getMessage());
			System.exit(-1);
		} catch (IOException e) {
			LOG.error(e.getMessage());
			System.exit(-1);
		}
		
		if (!isSingleTest()) { 
			// connect to Cassandra
			try {
				cluster = HFactory.getOrCreateCluster(getClusterName(),getHostIp());
				
				// check to see if the keyspace exists
				if (cluster.describeKeyspace(getKeyspace()) == null) { 
					cluster.addKeyspace(HFactory.createKeyspaceDefinition(getKeyspace()));
				} else { 
					LOG.info("keyspace: " + getKeyspace() + " exists");
				}
				
				KeyspaceDefinition keyspaceDef = cluster.describeKeyspace(getKeyspace());
				boolean cfExists = false;
				for (ColumnFamilyDefinition cfDef : keyspaceDef.getCfDefs()) {
					if (cfDef.getName().equals(getUserCf())) {
						cfExists = true;
					}
				}
			
				if (!cfExists) { 
					
					// create the new column family
					cluster.addColumnFamily(HFactory.createColumnFamilyDefinition(getKeyspace(), getUserCf()));

					// this populates a specific row with column names that we can pull a dynamic list (based on this row) to determine which columns need pulling
					String[] userCols = { "gender", "seeking_gender", "ethnicity", "hair", "eyes", "sexuality", "height", "weight", "income", "lifestyle", "living", "relationship", "bodytype", "children", "drinking", "smoking", "education", "exercise", "year", "day", "month", "age", "location", "browse_criteria", "online", "default_photo", "fullname"};
					StringSerializer ss = new StringSerializer();
					LongSerializer ls = new LongSerializer();
					Mutator<String> m = HFactory.createMutator(HFactory.createKeyspace(GenerateUsers.getKeyspace(), cluster), ss);
					for (int j = 0; j < userCols.length; j++) {
						m.addInsertion("user_cols", getUserCf(), HFactory.createColumn(userCols[j], System.nanoTime(), ss, ls));
					}
					m.execute();
				}
				
			} catch (HectorException he) { 
				LOG.error(he.getMessage());
				System.exit(1);
			}
			
			List<Thread> threads = new ArrayList<Thread>();
			for (int i = 0; i < getNumThreads(); i++) {
				Runnable task = new UserGeneration(getNumUsers());
				Thread worker = new Thread(task);
				worker.setName(String.valueOf(i));
				worker.start();
				threads.add(worker);			
			}
		} else { 
			Runnable task = new UserGeneration(getNumUsers());
			Thread worker = new Thread(task);
			worker.setName("singleTest");
			worker.start();		
		}
	} 


	public static String getKeyspace() {
		return keyspace;
	}


	public static void setKeyspace(String keyspace) {
		GenerateUsers.keyspace = keyspace;
	}


	public static String getUserRowKey() {
		return userRowKey;
	}


	public static void setUserRowKey(String userRowKey) {
		GenerateUsers.userRowKey = userRowKey;
	}


	public static String getUserCf() {
		return userCf;
	}


	public static void setUserCf(String userCf) {
		GenerateUsers.userCf = userCf;
	}


	public static String getHostIp() {
		return hostIp;
	}


	public static void setHostIp(String hostIp) {
		GenerateUsers.hostIp = hostIp;
	}


	public static String getClusterName() {
		return clusterName;
	}


	public static void setClusterName(String clusterName) {
		GenerateUsers.clusterName = clusterName;
	}


	public static int getNumUsers() {
		return numUsers;
	}


	public static void setNumUsers(int numUsers) {
		GenerateUsers.numUsers = numUsers;
	}


	public static int getNumThreads() {
		return numThreads;
	}


	public static void setNumThreads(int numThreads) {
		GenerateUsers.numThreads = numThreads;
	}


	public static boolean isSingleTest() {
		return singleTest;
	}


	public static void setSingleTest(boolean singleTest) {
		GenerateUsers.singleTest = singleTest;
	}


	public static Map<Integer, String> getFirstNames() {
		return firstNames;
	}


	public static Map<Integer, String> getLastNames() {
		return lastNames;
	}


	public static Random getGenerator() {
		return generator;
	}


	public static void setGenerator(Random generator) {
		GenerateUsers.generator = generator;
	}


	public static Cluster getCluster() {
		return cluster;
	}


	public static void setCluster(Cluster cluster) {
		GenerateUsers.cluster = cluster;
	}
	
	
	
}
