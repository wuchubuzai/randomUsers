package com.wuchubuzai.random;

import java.io.IOException;
import java.io.StringWriter;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
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

	private static final Logger LOG = LoggerFactory.getLogger(UserGeneration.class);	
	
	private static final StringSerializer STRING_SERIALIZER = new StringSerializer();
	private static final IntegerSerializer INTEGER_SERIALIZER = new IntegerSerializer();
	private static final LongSerializer LONG_SERIALIZER = new LongSerializer();
	private static ObjectMapper mapper = new ObjectMapper();
	private int numberOfUsers;
	private Map<String, int[]> userOptions = new HashMap<String, int[]>();
	static final SimpleDateFormat ISO8601FORMAT = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssZ");

	UserGeneration(int numUsers) { 
		this.numberOfUsers = numUsers;
		userOptions.put("gender", new int[] { 1,2 }); // male or female
		userOptions.put("seeking_gender", new int[] { 0,1,2 }); // no preference, male, female
		userOptions.put("ethnicity", new int[] { 0,1,2,3,4,5 }); // random "options"
		userOptions.put("hair", new int[] { 0,1,2,3,4,5 }); // random "options"
		userOptions.put("eyes", new int[] { 0,1,2,3,4,5 }); // random "options"
		userOptions.put("sexuality", new int[] { 0,1,2,3,4,5 }); // random "options"
		userOptions.put("income", new int[] { 0,1,2,3,4,5 }); // random "options"
		userOptions.put("lifestyle", new int[] { 0,1,2,3,4,5 }); // random "options"
		userOptions.put("living", new int[] { 0,1,2,3,4,5 }); // random "options"
		userOptions.put("relationship", new int[] { 0,1,2,3,4,5 }); // random "options"
		userOptions.put("bodytype", new int[] { 0,1,2,3,4,5 }); // random "options"
		userOptions.put("children", new int[] { 0,1,2,3,4,5 }); // random "options"
		userOptions.put("drinking", new int[] { 0,1,2,3,4,5 }); // random "options"
		userOptions.put("smoking", new int[] { 0,1,2,3,4,5 }); // random "options"
		userOptions.put("education", new int[] { 0,1,2,3,4,5 }); // random "options"
		userOptions.put("exercise", new int[] { 0,1,2,3,4,5 }); // random "options"
		userOptions.put(RandomUserConstants.YEAR_KEY, new int[] { 1910, 1984 } ); // used to specify the year of birth for a user
		userOptions.put("online", new int[] { 0, 1 } ); // 1 if the user is online
		userOptions.put("default_photo", new int[] { 0, 1 } ); // 0 if no photo, 1 if custom photo
		userOptions.put("proximity", new int[] { 0, 10, 50, 100, 500, 1000 } );
		userOptions.put("height", new int[] { 135,  210 } );
		userOptions.put("weight", new int[] { 39,  205 } );
	}
	
	public void run() {
		
		if (!GenerateUsers.isSingleTest()) { 
			Cluster cluster = GenerateUsers.getCluster();
			Keyspace ksp = HFactory.createKeyspace(GenerateUsers.getKeyspace(), cluster);
			
			long startTime = System.nanoTime();
			Mutator<String> m = HFactory.createMutator(ksp, STRING_SERIALIZER);
			
			int createdUsers = 0;
			
			for (int i = 0; i < numberOfUsers; i++) {
				Map<String, Object> user = generateUser();
				m.addInsertion(GenerateUsers.getUserRowKey(), GenerateUsers.getUserCf(), HFactory.createColumn(user.get(RandomUserConstants.ID_KEY).toString(), System.nanoTime(), STRING_SERIALIZER, LONG_SERIALIZER));
				m.addInsertion(user.get(RandomUserConstants.ID_KEY).toString(), GenerateUsers.getUserCf(), HFactory.createColumn(RandomUserConstants.ID_KEY, user.get(RandomUserConstants.ID_KEY).toString(), STRING_SERIALIZER, STRING_SERIALIZER));
				for (Map.Entry<String, Object> col : user.entrySet()) {
					if (col.getValue().getClass().toString().equals("class java.util.HashMap")) { 
						@SuppressWarnings("unchecked")
						HashMap<String, Object> userInfo = (HashMap<String, Object>) col.getValue();
						m.addInsertion(user.get(RandomUserConstants.ID_KEY).toString(), GenerateUsers.getUserCf(), HFactory.createColumn(col.getKey(), transformFieldToJson(userInfo).toString(), STRING_SERIALIZER, STRING_SERIALIZER));
					} else if (col.getValue().getClass().toString().equals("class java.lang.String")) { 
						m.addInsertion(user.get(RandomUserConstants.ID_KEY).toString(), GenerateUsers.getUserCf(), HFactory.createColumn(col.getKey(), col.getValue().toString(), STRING_SERIALIZER, STRING_SERIALIZER));
					} else if (col.getValue().getClass().toString().equals("class java.lang.Integer")) { 
						m.addInsertion(user.get(RandomUserConstants.ID_KEY).toString(), GenerateUsers.getUserCf(), HFactory.createColumn(col.getKey(), Integer.parseInt(col.getValue().toString()), STRING_SERIALIZER, INTEGER_SERIALIZER));
					}
					
			        if (i  == GenerateUsers.getGenerator().nextInt(numberOfUsers) && LOG.isDebugEnabled()) { 
			        	LOG.debug("loading random user " + i + " " + transformFieldToJson(user).toString());
			        }						
				}	
				// write out 20 users at a time 
				if (createdUsers == 20) { 
					m.execute();
					createdUsers = 0;
				} else { 
					createdUsers++;
				}
				
			}
			
			// insert all user records
			m.execute();
			long endTime = System.nanoTime();
			if (LOG.isDebugEnabled()) { 
				LOG.debug("execution time: " + (endTime - startTime));
			}
		} else { 
			if (LOG.isInfoEnabled()) {
				LOG.info("Single test requested");
			}
			Map<String, Object> user = generateUser();
			if (LOG.isInfoEnabled()) { 
				LOG.info(transformFieldToJson(user).toString());
			}
		}
		
	}	
	

	public Map<String, Object> generateUser() { 
		Map<String, Object> user = new HashMap<String, Object>();
		
		// generate a new UUID for the user
		UUID uid = UUID.randomUUID();
		user.put(RandomUserConstants.ID_KEY, uid.toString());
		
		// used to store the user's "browse criteria"
		Map<String, Object> bc = new HashMap<String, Object>();
		
		// iterate through all user options 
		for (Entry<String, int[]> option : this.userOptions.entrySet()) {
			
			if (option.getKey().equals(RandomUserConstants.YEAR_KEY)) {
				
					// generate random year, month day for user's birthday
				int year = GenerateUsers.getGenerator().nextInt(option.getValue()[1] - option.getValue()[0]) + option.getValue()[0];
				user.put(RandomUserConstants.YEAR_KEY, year);
				
				int month = GenerateUsers.getGenerator().nextInt(12-1) + 1;
				user.put("month", month);
				
				int day = GenerateUsers.getGenerator().nextInt(29-1) + 1;
				user.put("day", day);  
					
				// generate the user's age as of today
				GregorianCalendar then = new GregorianCalendar(year, month, day);
				Date nowDate = new Date();
				GregorianCalendar now = new GregorianCalendar();
				now.setTimeInMillis(nowDate.getTime());
				
				user.put("age", now.get(GregorianCalendar.YEAR) - then.get(GregorianCalendar.YEAR));
			       
			} else { 
				
				if (!option.getKey().equals("height") && !option.getKey().equals("weight")) { 
					// select a random option
					int val = option.getValue()[GenerateUsers.getGenerator().nextInt(option.getValue().length)];
					
					// if the option value is greater than 0, add it
					if (val > 0) { 
						if (option.getKey().equals("default_photo")) { 
							user.put(option.getKey(), "non-default photo");
//						} else if (option.getKey().equals("proximity")) { 
							 // only used for browse criteria -- 
						} else { 
							user.put(option.getKey(), val);
						}
					} else { 
						if (option.getKey().equals("default_photo")) { 
							user.put(option.getKey(), RandomUserConstants.DEFAULT_KEY); 
						}
					}
					
					// select another random option to populate browse criteria
					int bcVal = option.getValue()[GenerateUsers.getGenerator().nextInt(option.getValue().length)];
					
					// if the option value is greater than 0, add it
					if (bcVal > 0) { 
						bc.put(option.getKey(), bcVal);
					}
				} else { 
					// generate user height & weight
					int val = option.getValue()[0] + (int)(Math.random() * ((option.getValue()[1] - option.getValue()[0]) + 1));
					user.put(option.getKey(), val);
				}
			}
		}
		
		// generate a random name for this user
		user.put("fullname", GenerateUsers.getFirstNames().get(GenerateUsers.getGenerator().nextInt(GenerateUsers.getFirstNames().size())) + " " + GenerateUsers.getLastNames().get(GenerateUsers.getGenerator().nextInt(GenerateUsers.getLastNames().size())));
		
		// generate some geo-location data so that we can test out haversine 
		double minLat = -90.00000;
		double maxLat = 90.00000;
		double latitude = minLat + (double)(Math.random() * ((maxLat - minLat) + 1));

		double minLon = 0.00000;
		double maxLon = 180.00000;
		double longitude = minLon + (double)(Math.random() * ((maxLon - minLon) + 1));
		
		DecimalFormat df = new DecimalFormat("#.#####");		
		Map<String, Object> coords = new HashMap<String, Object>();
		coords.put("latitude", df.format(latitude).toString());
		coords.put("longitude", df.format(longitude).toString());
        Date now = new Date();
		coords.put("date", ISO8601FORMAT.format(now));
		user.put("location", coords);	

		// save the browse criteria
		user.put("browse_criteria", bc);
			
		return user;
	}
	
	public StringWriter transformFieldToJson(Map<String, Object> field) { 
		StringWriter sw = new StringWriter();
		try {
			mapper.writeValue(sw, field);
		} catch (JsonGenerationException e) {
			LOG.error("JsonGenerationException: " + e.getMessage());
		} catch (JsonMappingException e) {
			LOG.error("JsonMappingException: " + e.getMessage());
		} catch (IOException e) {
			LOG.error("IOException: " + e.getMessage());
		}
		return sw;
	}
	
}
