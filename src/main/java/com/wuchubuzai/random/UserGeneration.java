package com.wuchubuzai.random;

import java.io.IOException;
import java.io.StringWriter;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
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
	static final SimpleDateFormat ISO8601FORMAT = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssZ");

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
		userOptions.put("year", new int[] { 1910, 1984 } );
	}
	
	public void run() {
		
		if (!GenerateUsers.isSingleTest()) { 
			Cluster cluster = GenerateUsers.cluster;
			Keyspace ksp = HFactory.createKeyspace(GenerateUsers.getKeyspace(), cluster);
			
			long startTime = System.nanoTime();
			Mutator<String> m = HFactory.createMutator(ksp, ss);
			for (int i = 0; i < NUM_USERS; i++) {
				Map<String, Object> user = generateUser();

				m.addInsertion(GenerateUsers.getUserRowKey(), GenerateUsers.getUserCf(), HFactory.createColumn(user.get("id").toString(), System.nanoTime(), ss, ls));
				m.addInsertion(user.get("id").toString(), GenerateUsers.getUserCf(), HFactory.createColumn("id", user.get("id").toString(), ss, ss));
				for (Map.Entry<String, Object> col : user.entrySet()) {
					log.info(col.getValue().getClass().toString());
					if (col.getValue().getClass().equals("java.util.HashMap")) { 
						@SuppressWarnings("unchecked")
						HashMap<String, Object> userInfo = (HashMap<String, Object>) col.getValue();
						m.addInsertion(user.get("id").toString(), GenerateUsers.getUserCf(), HFactory.createColumn(col.getKey(), transformFieldToJson(userInfo).toString(), ss, ss));
					} else if (col.getValue().getClass().equals("java.util.String")) { 
						m.addInsertion(user.get("id").toString(), GenerateUsers.getUserCf(), HFactory.createColumn(col.getKey(), col.getValue().toString(), ss, ss));
					} else if (col.getValue().getClass().equals("java.util.Integer")) { 
						m.addInsertion(user.get("id").toString(), GenerateUsers.getUserCf(), HFactory.createColumn(col.getKey(), Integer.parseInt(col.getValue().toString()), ss, is));
					}
				}		
			}
			
			// insert all user records
			m.execute();
			long endTime = System.nanoTime();
			if (log.isDebugEnabled()) log.debug("execution time: " + (endTime - startTime));
		} else { 
			if (log.isInfoEnabled()) log.info("Single test requested");
			Map<String, Object> user = generateUser();
			if (log.isInfoEnabled()) log.info(transformFieldToJson(user).toString());
		}
		
	}	
	

	public Map<String, Object> generateUser() { 
		Map<String, Object> user = new HashMap<String, Object>();
		
		// generate a new UUID for the user
		UUID uid = UUID.randomUUID();
		user.put("id", uid.toString());
		
		// used to store the user's "browse criteria"
		Map<String, Object> bc = new HashMap<String, Object>();
		
		// iterate through all user options 
		for (Entry<String, int[]> option : this.userOptions.entrySet()) {
			
			if (option.getKey().equals("year")) {
				
					// generate random year, month day for user's birthday
				int year = GenerateUsers.generator.nextInt(option.getValue()[1] - option.getValue()[0]) + option.getValue()[0];
				user.put("year", year);
				
				int month = GenerateUsers.generator.nextInt(12-1) + 1;
				user.put("month", month);
				
				int day = GenerateUsers.generator.nextInt(29-1) + 1;
				user.put("day", day);  
					
				// generate the user's age as of today
				GregorianCalendar then = new GregorianCalendar(year, month, day);
				Date nowDate = new Date();
				GregorianCalendar now = new GregorianCalendar();
				now.setTimeInMillis(nowDate.getTime());
				
				user.put("age", now.get(GregorianCalendar.YEAR) - then.get(GregorianCalendar.YEAR));
			       
			} else { 
				// select a random option
				int val = option.getValue()[GenerateUsers.generator.nextInt(option.getValue().length)];
				
				// if the option value is greater than 0, add it
				if (val > 0) user.put(option.getKey(), val); 
				
				// select another random option to populate browse criteria
				int bcVal = option.getValue()[GenerateUsers.generator.nextInt(option.getValue().length)];
				
				// if the option value is greater than 0, add it
				if (bcVal > 0) bc.put(option.getKey(), bcVal);
			}
		}
		
		// generate some geo-location data so that we can test out haversine 
		double minLat = -90;
		double maxLat = 90;
		double latitude = minLat + (double)(Math.random() * ((maxLat - minLat) + 1));

		double minLon = 0;
		double maxLon = 180;			
		double longitude = minLon + (double)(Math.random() * ((maxLon - minLon) + 1));
		
		DecimalFormat df = new DecimalFormat("#.#####");		
		Map<String, Object> coords = new HashMap<String, Object>();
		coords.put("latitude", df.format(latitude).toString());
		coords.put("longitude", df.format(longitude).toString());
        Date now = new Date();
		coords.put("date", ISO8601FORMAT.format(now));
		user.put("location", transformFieldToJson(coords).toString());	

		// save the browse criteria
		user.put("browse_criteria", transformFieldToJson(bc).toString());
			
		return user;
	}
	
	public StringWriter transformFieldToJson(Map<String, Object> field) { 
		StringWriter sw = new StringWriter();
		try {
			mapper.writeValue(sw, field);
		} catch (JsonGenerationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (JsonMappingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return sw;
	}
	
}
