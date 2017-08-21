package org.apache.solr.handler.dataimport;


import com.mongodb.*;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;

import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

import static org.apache.solr.handler.dataimport.DataImportHandlerException.SEVERE;
import static org.apache.solr.handler.dataimport.DataImportHandlerException.wrapAndThrow;

/**
 * Solr MongoDB data source
 *
 */
public class MongoDataSource extends DataSource<Iterator<Map<String, Object>>> {

	private static final Logger LOG = LoggerFactory.getLogger(DataSource.class);
	
	private MongoCollection mongoCollection;
	private MongoDatabase mongoDb;
	private MongoClient mongoClient;
	private MongoCursor mongoCursor;
	private String mapMongoFields;
	private Integer limit;
	
	/**
	 * Initialize
	 * 
	 * @param context
	 * @param initProps
	 */
	@Override
	public void init(Context context, Properties initProps) {
		
		String databaseName = initProps.getProperty(DATABASE);
		String host = initProps.getProperty(HOST, "localhost");
		String port = initProps.getProperty(PORT, "27017");
		String username = initProps.getProperty(USERNAME);
		String password = initProps.getProperty(PASSWORD);

		mapMongoFields = initProps.getProperty(MAP_MONGO_FIELDS, "true");


		List<ServerAddress> seeds = new ArrayList<>();
		String[] hosts = host.split(",");
		String[] ports = port.split(",");

		for (int i = 0; i < hosts.length; i++) {
			String seed_port = hosts.length == ports.length ? ports[i] : ports[0];
			seeds.add(new ServerAddress(hosts[i],Integer.parseInt(seed_port)));
		}

		if (databaseName == null) {
			throw new DataImportHandlerException(SEVERE, "Database must be supplied");
		}

		MongoClientOptions clientOptions = MongoClientOptions.builder().
				readPreference(ReadPreference.secondaryPreferred()).build();
		try {
			if (username != null) {
				MongoCredential credential = MongoCredential.createCredential(username, databaseName, password.toCharArray());

				this.mongoClient = new MongoClient(seeds, Arrays.asList(credential),clientOptions);
			} else {
				this.mongoClient = new MongoClient(seeds,clientOptions);
			}


			this.mongoDb = this.mongoClient.getDatabase(databaseName);
                        this.mongoDb = this.mongoDb.withReadPreference(ReadPreference.secondaryPreferred());
		} catch (RuntimeException e) {
			throw e;
		} catch (Exception e) {
			throw new DataImportHandlerException(SEVERE, "Unable to connect to Mongo");
		}
	}

	/**
	 * Get data by query
	 * 
	 * @param query
	 * @return 
	 */
	@Override
	public Iterator<Map<String, Object>> getData(String query) {
		LOG.info("Started data import for query: " + query);

		BasicDBObject queryObject = BasicDBObject.parse(query);

		mongoCursor = this.mongoCollection.find(queryObject).limit(limit).iterator();

		ResultSetIterator resultSet = new ResultSetIterator(mongoCursor);
		return resultSet.getIterator();
	}

	/**
	 * Get data
	 * 
	 * @param query
	 * @param collection
	 * @return 
	 */
	public Iterator<Map<String, Object>> getData(String query, String collection, Integer limit) {
		this.mongoCollection = this.mongoDb.getCollection(collection);
		this.limit = limit;
		return getData(query);
	}

	/**
	 * Result set iterator
	 * 
	 */
	private class ResultSetIterator {

		MongoCursor mongoCursor;

		Map<String, String> dateFields;
		
		Iterator<Map<String, Object>> resultSetIterator;

		public ResultSetIterator(MongoCursor mongoCursor) {
			this.mongoCursor = mongoCursor;

			resultSetIterator = new Iterator<Map<String, Object>>() {
				@Override
				public boolean hasNext() {
					return hasnext();
				}

				@Override
				public Map<String, Object> next() {
					if (!hasNext()) {
						throw new NoSuchElementException();
					}
					return getARow();
				}

				@Override
				public void remove() {

				}
			};

		}

		/**
		 * Get iterator
		 * 
		 * @return 
		 */
		public Iterator<Map<String, Object>> getIterator() {
			return resultSetIterator;
		}

		/**
		 * Get a data row
		 * 
		 * @return 
		 */
		private Map<String, Object> getARow() {
			Document document = (Document) getMongoCursor().next();
			Map<String, Object> result = new HashMap<String, Object>();

			Set<String> keys;

			if("true".equals(mapMongoFields)){
				keys = getDocumentKeys(document, null);
			}else{
				keys = document.keySet();
			}

			Iterator<String> keysIterator = keys.iterator();

			while (keysIterator.hasNext()) {
				String key = keysIterator.next();

				Object innerObject = getDocumentFieldValue(document, key);

				innerObject = serializeObject(innerObject);

				result.put(key, innerObject);
			}

			return result;
		}

		private Object serializeObject(Object object){
			Object response;
			if(object instanceof ArrayList){
				response = new ArrayList();
				for (int i = 0; i < ((ArrayList) object).size(); i++) {
					((ArrayList)response).add(serializeObject(((ArrayList) object).get(i)));
				}
			}else if(object instanceof Document){
				response = Document.parse(((Document) object).toJson());
			}else{
				response = object;
			}
			return response;
		}

		/**
		 * Get keys of a document
		 * 
		 * @param doc
		 * @param parentKey
		 * @return 
		 */
		private Set<String> getDocumentKeys(Document doc, String parentKey) {
			Set<String> keys = new HashSet<String>();

			Set<String> docKeys = doc.keySet();

			Iterator<String> docKeysIterator = docKeys.iterator();

			while (docKeysIterator.hasNext()) {
				String docKey = docKeysIterator.next();
				
				Object docPart = doc.get(docKey);
				
				if (docPart instanceof Document) {
					String parent;

					if (parentKey == null) {
						parent = docKey;
					} else {
						parent = parentKey + "." + docKey;
					}

					Set<String> subKeys = getDocumentKeys((Document) docPart, parent);

					Iterator<String> subKeysIterator = subKeys.iterator();

					while (subKeysIterator.hasNext()) {
						keys.add(subKeysIterator.next());
					}
				} else if (docPart instanceof ArrayList) {
					Iterator<Object> listIterator = ((ArrayList) docPart).iterator();

					Integer i = 0;
					Boolean isScalarValuesOnly = true;

					while (listIterator.hasNext()) {
						String parent;

						if (parentKey == null) {
							parent = docKey + "." + i.toString();
						} else {
							parent = parentKey + "." + docKey + "." + i.toString();
						}

						Object listItem = listIterator.next();

						if (listItem instanceof Document || listItem instanceof ArrayList) {
							Set<String> subKeys = getDocumentKeys((Document) listItem, parent);

							Iterator<String> subKeysIterator = subKeys.iterator();

							while (subKeysIterator.hasNext()) {
								keys.add(subKeysIterator.next());
							}
							
							if (isScalarValuesOnly) {
								isScalarValuesOnly = false;
							}
						}

						i++;
					}
					
					if (isScalarValuesOnly) {
						if (parentKey != null) {
							keys.add(parentKey + "." + docKey);
						} else {
							keys.add(docKey);
						}
					}
				} else {
					if (parentKey != null) {
						keys.add(parentKey + "." + docKey);
					} else {
						keys.add(docKey);
					}
				}
			}
			return keys;
		}

		/**
		 * Get a document filed value by field name
		 * 
		 * @param object
		 * @param fieldName
		 * @return 
		 */
		private Object getDocumentFieldValue(Document object, String fieldName) {

			final String[] fieldParts = fieldName.split("\\.");

			int i = 1;
			Object value = object.get(fieldParts[0]);

			while (i < fieldParts.length && (value instanceof Document || value instanceof ArrayList)) {
				if (value instanceof ArrayList) {
					value = ((ArrayList) value).get(Integer.parseInt(fieldParts[i]));
				} else {
					value = ((Document) value).get(fieldParts[i]);
				}
				i++;
			}
			
			return value;
		}
		
		/**
		 * Check if result set has next record
		 * 
		 * @return 
		 */
		private boolean hasnext() {
			if (mongoCursor == null) {
				return false;
			}
			try {
				if (mongoCursor.hasNext()) {
					return true;
				} else {
					close();
					return false;
				}
			} catch (MongoException e) {
				close();
				wrapAndThrow(SEVERE, e);
				return false;
			}
		}
		
		/**
		 * Close cursor
		 * 
		 */
		private void close() {
			try {
				if (mongoCursor != null) {
					mongoCursor.close();
				}
			} catch (Exception e) {
				LOG.warn("Exception while closing result set", e);
			} finally {
				mongoCursor = null;
			}
		}
	}

	/**
	 * Get MongoDB cursor
	 * 
	 * @return 
	 */
	private MongoCursor getMongoCursor() {
		return this.mongoCursor;
	}

	/**
	 * Close
	 * 
	 */
	@Override
	public void close() {
		if (this.mongoCursor != null) {
			this.mongoCursor.close();
		}

		if (this.mongoClient != null) {
			this.mongoClient.close();
		}
	}

	/**
	 * Database option
	 * 
	 */
	private static final String DATABASE = "database";
	
	/**
	 * Host option
	 * 
	 */
	private static final String HOST = "host";
	
	/**
	 * Port option
	 * 
	 */
	private static final String PORT = "port";
	
	/**
	 * Username option
	 * 
	 */
	private static final String USERNAME = "username";
	
	/**
	 * Password option
	 * 
	 */
	private static final String PASSWORD = "password";

	/**
	 * Map Mongo Fields option
	 *
	 */
	private static final String MAP_MONGO_FIELDS = "mapMongoFields";

		
}
