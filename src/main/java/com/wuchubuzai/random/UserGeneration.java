package com.wuchubuzai.random;

import java.io.IOException;
import java.io.StringWriter;
import java.io.Writer;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.UUID;

import me.prettyprint.cassandra.serializers.IntegerSerializer;
import me.prettyprint.cassandra.serializers.LongSerializer;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.mutation.Mutator;

import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Create random users!  
 * @author sd
 *
 */
public class UserGeneration implements Runnable {

	static final Logger log = LoggerFactory.getLogger(UserGeneration.class);	
	
	static final StringSerializer ss = new StringSerializer();
	static final IntegerSerializer is = new IntegerSerializer();
	static final LongSerializer ls = new LongSerializer();
	private static ObjectMapper mapper = new ObjectMapper();
	private final int NUM_USERS;
	private final Map<String, int[]> userOptions = new HashMap<String, int[]>();
	Random generator = new Random();


	UserGeneration(int numUsers) { 
		this.NUM_USERS = numUsers;
		userOptions.put("gender", new int[] { 1,2 });
		userOptions.put("seeking_gender", new int[] { 0,1,2 });
		userOptions.put("ethnicity", new int[] { 0,1,2,3,4,5 });
		userOptions.put("hair", new int[] { 0,1,2,3,4,5 });
		userOptions.put("eyes", new int[] { 0,1,2,3,4,5 });
		userOptions.put("sexuality", new int[] { 0,1,2,3,4,5 });
		userOptions.put("income", new int[] { 0,1,2,3,4,5 });
		userOptions.put("lifestyle", new int[] { 0,1,2,3,4,5 });
		userOptions.put("living", new int[] { 0,1,2,3,4,5 });
		userOptions.put("relationship", new int[] { 0,1,2,3,4,5 });
		userOptions.put("bodytype", new int[] { 0,1,2,3,4,5 });
		userOptions.put("children", new int[] { 0,1,2,3,4,5 });
		userOptions.put("drinking", new int[] { 0,1,2,3,4,5 });
		userOptions.put("smoking", new int[] { 0,1,2,3,4,5 });
		userOptions.put("education", new int[] { 0,1,2,3,4,5 });
		userOptions.put("exercise", new int[] { 0,1,2,3,4,5 });
	}
	
	public void run() {
		
		Cluster cluster = GenerateUsers.cluster;
		Keyspace ksp = HFactory.createKeyspace(GenerateUsers.KEYSPACE, cluster);
		
		long startTime = System.nanoTime();
		Mutator<String> m = HFactory.createMutator(ksp, ss);
		for (int i = 0; i < NUM_USERS; i++) {
			UUID uid = UUID.randomUUID();
			
			// used to store the user's "browse criteria"
			Map<String, Integer> bc = new HashMap<String, Integer>();
			
			// iterate through all user options 
			for (Entry<String, int[]> option : this.userOptions.entrySet()) {
				
				// select a random option
				int val = option.getValue()[generator.nextInt(option.getValue().length)];
				
				// if the option value is greater than 0, add it
				if (val > 0) m.addInsertion(uid.toString(), GenerateUsers.USER_CF, HFactory.createColumn(option.getKey(), val, ss, is));
				
				// select another random option to populate browse criteria
				int bcVal = option.getValue()[generator.nextInt(option.getValue().length)];
				
				// if the option value is greater than 0, add it
				if (bcVal > 0) bc.put(option.getKey(), bcVal);
			}
						
			
			try {
				Writer sw = new StringWriter();

				// convert the browse criteria into a JSON string
				mapper.writeValue(sw, bc);
				m.addInsertion(GenerateUsers.USER_ROW_KEY, GenerateUsers.USER_CF, HFactory.createColumn("browse_criteria", sw.toString(), ss, ss));	
				
				// for information purposes, print out the 50th user's browse criteria
				if (i == 50 && log.isInfoEnabled()) log.info(uid.toString() + " browse criteria: " + sw.toString()); 
				
			} catch (JsonGenerationException e) {
				e.printStackTrace();
			} catch (JsonMappingException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			}
			
			// populate the newly added user id to a single row for easy counting of total users.
			m.addInsertion(GenerateUsers.USER_ROW_KEY, GenerateUsers.USER_CF, HFactory.createColumn(uid.toString(), System.nanoTime(), ss, ls));			
		}
		
		
		// insert all user records
		m.execute();
		long endTime = System.nanoTime();
		if (log.isDebugEnabled()) log.debug("execution time: " + (endTime - startTime));
		
	}	
	

	
	
}
