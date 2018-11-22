import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Properties;

import org.bson.Document;

import com.google.common.util.concurrent.RateLimiter;
import com.mongodb.BasicDBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.MongoException;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;

/** 
 *  GetWeather class get weather info from openweathermap and save them in mongoDB data
 *
 * @author  Nicolas Girault
 * @version 1.0
 * @since   2018-10-25 
 */
public class GetWeather {
	
	static final String PROPERTY_FILE_LOC = "src/main/resources/config.properties";
	static String appId = null;
	static int limitRequest = 60;
	static String[] cities;
	
	static MongoClient mongoClient;
	static MongoCollection<Document> collection;
	static final String MONGO_URI = "mongodb+srv://Admin:nimda8102@cluster0-cbecc.mongodb.net/admin";

	static final String BASE_URL_OPENWEATHER = "http://api.openweathermap.org/data/2.5/weather?q=";
			
	public static void main(String[] args) {
		// Get Properties File info
		getPropertiesInfo();
							        
		RateLimiter limiter = RateLimiter.create(1.0/(60/limitRequest));	        
		// Call openweathermap
        while(true){
		getDataFromOpenWeatherMap(limiter);
		}		
	}

	private static void getPropertiesInfo() {						
		try (InputStream in = new FileInputStream(PROPERTY_FILE_LOC)) {
			Properties prop = new Properties();				
			prop.load(in);

			cities=prop.getProperty("cities").split(",");				
			appId=prop.getProperty("appId");				
			limitRequest=Integer.parseInt(prop.getProperty("limitrequest"));
			
		} catch (IOException e) {					           
		            e.printStackTrace();
		}			
	}
	
	private static void getDataFromOpenWeatherMap(RateLimiter limiter) {			
		try {
			// Open MongoDB Connection
			openMongoDB();
			
			// Call API
			for (String c: cities) {						
				URL url = new URL(BASE_URL_OPENWEATHER+c+"&APPID="+appId+"&units=imperial");					
				limiter.acquire();				
				HttpURLConnection conn = (HttpURLConnection) url.openConnection();
				conn.setRequestMethod("GET");
				conn.setRequestProperty("Accept", "application/json");
	
				if (conn.getResponseCode() != 200) {
					throw new RuntimeException("Failed : HTTP error code : "+ conn.getResponseCode());
				}
		
				BufferedReader br = new BufferedReader(new InputStreamReader((conn.getInputStream())));
				String resultJson = org.apache.commons.io.IOUtils.toString(br);				
				conn.disconnect();
				
				//Save to MongoDB
				saveToMongoDB(c,resultJson);												
			}
			
			// Close MongoDB Connection
			closeMongoDB();	
			
		} catch (MalformedURLException e) {
		e.printStackTrace();
		} catch (IOException e) {
		e.printStackTrace();
		} catch (MongoException e) {
		e.printStackTrace();
	    }
				
	}

	private static void openMongoDB() {
		MongoClientURI clientURI = new MongoClientURI(MONGO_URI);
		mongoClient = new MongoClient(clientURI);
		
		MongoDatabase mongoDatabase = mongoClient.getDatabase("MyMongoDB");
		collection = mongoDatabase.getCollection("weather");			
	}

	private static void closeMongoDB() {
		mongoClient.close();			
	}

	private static void saveToMongoDB(String cityLocation, String resultJson) {
		BasicDBObject weatherInfo = BasicDBObject.parse(resultJson);
			
		collection.deleteOne(Filters.eq("name",cityLocation));				
		collection.insertOne(new Document(weatherInfo));	
	}	
	
}
