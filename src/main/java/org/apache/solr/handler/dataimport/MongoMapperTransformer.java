/**
 * Solr MongoDB data import handler
 *
 */
package org.apache.solr.handler.dataimport;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;
import java.util.Map;

import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * MongoDB mapper transformer
 *
 */
public class MongoMapperTransformer extends Transformer {

	/**
	 * Solr date format
	 *
	 */
	public static final String SOLR_DATE_FORMAT = "yyyy-MM-dd'T'HH:mm:ss'Z'";
	private static final Logger LOG = LoggerFactory.getLogger(Transformer.class);
	private static final DateFormat solrDateFormat = new SimpleDateFormat(SOLR_DATE_FORMAT);


	/**
	 * Transform row
	 *
	 * @param row
	 * @param context
	 * @return
	 */
	@Override
	public Object transformRow(Map<String, Object> row, Context context) {


		for (String rowKey : row.keySet()) {

			if( row.get(rowKey) instanceof ObjectId) {
				row.put(rowKey, ((ObjectId) row.get(rowKey)).toHexString());
			}
		}

		for (Map<String, String> map : context.getAllEntityFields()) {
			String mongoFieldName = map.get(MONGO_FIELD);

			if (mongoFieldName == null) {
				continue;
			}

			String columnFieldName = map.get(DataImporter.COLUMN);

			Object value = row.get(mongoFieldName);

			String columnDateFormat = map.get(DATE_FORMAT);


			/**
			 * Convert date to required format
			 *
			 */
			if (columnDateFormat != null && value instanceof String) {
				try {

					DateFormat dateFormat = new SimpleDateFormat(columnDateFormat, Locale.US);
					dateFormat.setTimeZone(java.util.TimeZone.getTimeZone("UTC"));

					Date date = dateFormat.parse(value.toString());

					value = solrDateFormat.format(date);

				} catch (Exception e) {
					LOG.warn("Date conversion error", e);

					value = null;
				}
			}

			row.put(columnFieldName, value);


		}
		return row;
	}

	/**
	 * MongoDB field option
	 *
	 */
	public static final String MONGO_FIELD = "mongoField";

	/**
	 * Date format option
	 *
	 */
	public static final String DATE_FORMAT = "dateFormat";

}
