package javaDisplayWeather;

import java.io.IOException;
import java.util.Date;
import org.bson.Document;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.MongoException;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;

/** 
 *  DisplayCurrentWeather class displays weather information from mongoDB data
 *  
 * @author  Nicolas Girault
 * @version 1.0
 * @since   2018-10-25 
 */

public class DisplayCurrentWeather {
	
	static final String MONGODB_URI = "mongodb+srv://Admin:nimda8102@cluster0-cbecc.mongodb.net/admin";

	public static void main(String[] args) {

	  try {
		  // Connect MongoDb
		  MongoClientURI clientURI = new MongoClientURI(MONGODB_URI);
		  MongoClient mongoClient = new MongoClient(clientURI);

		  MongoDatabase mongoDatabase = mongoClient.getDatabase("MyMongoDB");
		  MongoCollection < Document > collection = mongoDatabase.getCollection("weather");

		  MongoCursor < Document > cursor = collection.find().iterator();
		  System.out.println("Current Weather (" + new Date() + ") is :");
		  try {
			  while (cursor.hasNext()) {
				  // Get data from Json & display
				  displayCleanData(cursor.next().toJson());
			  }
		   } finally {
		    cursor.close();
		   }

		  // Close MongoDb connection	
		  mongoClient.close();
		  
	  	} catch (MongoException e) {
		e.printStackTrace();
	  	} catch (Exception e) {
	    System.out.println(e);
	  }

	 }

	 private static void displayCleanData(String json) throws JsonParseException, JsonMappingException, IOException {
	  //create ObjectMapper instance
	  ObjectMapper objectMapper = new ObjectMapper();
	  
	  //convert json string to object
	  Location loc = objectMapper.readValue(json, Location.class);
	  
	  // display
	  System.out.println("\n" + loc);
	 }
	}
