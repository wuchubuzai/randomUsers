package com.wuchubuzai.random;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.ddl.ColumnFamilyDefinition;
import me.prettyprint.hector.api.ddl.KeyspaceDefinition;
import me.prettyprint.hector.api.exceptions.HectorException;
import me.prettyprint.hector.api.factory.HFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Generate random users! 
 * @author sd
 *
 */
public class GenerateUsers {

	static final Logger log = LoggerFactory.getLogger(GenerateUsers.class);
	
	// configure these as you like.
	final static String KEYSPACE = "random";
	final static String USER_ROW_KEY = "users";
	final static String USER_CF = "user";
	final static String HOST_IP = "127.0.0.1:9160";
	final static String CLUSTER_NAME = "cluster";
	
	// create 25,000 user entries
	final static int NUM_USERS = 250;
	final static int NUM_THREADS = 100;
	
	static Cluster cluster;
	static Random generator = new Random();
	
	
	public static void main(String[] args) {

		// connect to Cassandra
		try {
			cluster = HFactory.getOrCreateCluster(CLUSTER_NAME,HOST_IP);
			
			// check to see if the keyspace exists
			if (cluster.describeKeyspace(KEYSPACE) == null) { 
				cluster.addKeyspace(HFactory.createKeyspaceDefinition(KEYSPACE));
			} else { 
				log.info("keyspace: " + KEYSPACE + " exists");
			}
			
			
			
			KeyspaceDefinition keyspaceDef = cluster.describeKeyspace(KEYSPACE);
			boolean cfExists = false;
			for (ColumnFamilyDefinition cfDef : keyspaceDef.getCfDefs()) {
				if (cfDef.getName().equals(USER_CF)) cfExists = true;
			}
		
			if (!cfExists) cluster.addColumnFamily(HFactory.createColumnFamilyDefinition(KEYSPACE, USER_CF));
			
		} catch (HectorException he) { 
			log.error(he.getMessage());
			System.exit(1);
		}
		
		
		List<Thread> threads = new ArrayList<Thread>();
		for (int i = 0; i < NUM_THREADS; i++) {
			Runnable task = new UserGeneration(NUM_USERS);
			Thread worker = new Thread(task);
			worker.setName(String.valueOf(i));
			worker.start();
			threads.add(worker);			
		}
	}
}
