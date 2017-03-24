package com.cds.utils.jackson;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.dataformat.csv.CsvFactory;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;
import com.fasterxml.jackson.dataformat.smile.SmileFactory;
import com.fasterxml.jackson.dataformat.xml.XmlFactory;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import java.io.IOException;

/**
 * <p>jackson解析类，可以用来解析json、json smile、XML、YAML and CSV</p>
 *
 * @author chendongsheng5 2017/1/5 16:48
 * @version V1.0
 * @modificationHistory =========================逻辑或功能性重大变更记录
 * @modify by user: chendongsheng5 2017/1/5 16:48
 * @modify by reason:{方法名}:{原因}
 */
public class ObjectMapperFactory {

	//The modifiable Jackson object mapper
	private volatile static ObjectMapper objectMapper;

	//The modifiable Jackson Object Writer
	private volatile static ObjectWriter objectWriter;

	//The modifiable Jackson Object Reader
	private volatile static ObjectReader objectReader;

	private static ObjectMapper create(JsonFactory jsonFactory) {
		ObjectMapper mapper = jsonFactory == null ? new ObjectMapper() : new ObjectMapper(jsonFactory);

		// TODO: 添加注释 
		mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
		mapper.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
		mapper.configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);
		mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
		mapper.configure(JsonGenerator.Feature.AUTO_CLOSE_TARGET, false);
		return mapper;
	}

	private static CsvSchema createCsvSchema(CsvMapper csvMapper, Class<?> clazz) {
		return csvMapper.schemaFor(clazz);
	}

	public static ObjectMapper getObjectMapper(MapperType mapperType) {
		if (null == objectMapper) {
			objectMapper = createObjectMapper(mapperType);
		}
		return objectMapper;
	}

	public static ObjectWriter getObjectWriter(MapperType mapperType, Class<?> clazz) throws IOException {
		if (null == objectWriter) {
			objectWriter = createObjectWriter(mapperType, clazz);
		}
		return objectWriter;
	}

	public static ObjectReader getObjectReader(MapperType mapperType, Class<?> clazz) throws IOException {
		if (null == objectReader) {
			objectReader = createObjectReader(mapperType, clazz);
		}
		return objectReader;
	}

	private static ObjectMapper createObjectMapper(MapperType mapperType) {
		ObjectMapper result = null;

		if (null == mapperType) {
			return null;
		}

		if (mapperType == MapperType.YAML) {
			result = create(new YAMLFactory());
		} else if (mapperType == MapperType.JSON) {
			result = create(new JsonFactory());
		} else if (mapperType == MapperType.CSV) {
			result = create(new CsvFactory());
		} else if (mapperType == MapperType.SMILE) {
			result = create(new SmileFactory());
		} else if (mapperType == MapperType.XML) {
			result = create(new XmlFactory());
		}

		return result;
	}

	private static ObjectReader createObjectReader(MapperType mapperType, Class<?> clazz) throws IOException {
		ObjectReader result;
		if (null == mapperType) {
			return null;
		}

		if (mapperType == MapperType.CSV) {
			CsvMapper csvMapper = (CsvMapper) getObjectMapper(mapperType);
			CsvSchema csvSchema = createCsvSchema(csvMapper, clazz);
			result = csvMapper.reader(clazz).with(csvSchema);
		} else {
			result = getObjectMapper(mapperType).reader(clazz);
		}
		return result;
	}

	private static ObjectWriter createObjectWriter(MapperType mapperType, Class<?> clazz) throws IOException {
		ObjectWriter result;
		if (null == mapperType) {
			return null;
		}

		if (mapperType == MapperType.CSV) {
			CsvMapper csvMapper = (CsvMapper) getObjectMapper(mapperType);
			CsvSchema csvSchema = createCsvSchema(csvMapper, clazz);
			result = csvMapper.writer(csvSchema);
		} else {
			result = getObjectMapper(mapperType).writerWithType(clazz);
		}

		return result;
	}
}
