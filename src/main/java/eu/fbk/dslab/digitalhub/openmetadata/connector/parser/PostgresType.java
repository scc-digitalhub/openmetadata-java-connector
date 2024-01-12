package eu.fbk.dslab.digitalhub.openmetadata.connector.parser;

import org.openmetadata.client.model.Column;

public class PostgresType {
	public static Column.DataTypeEnum getDataType(String type) {
		if(type.equalsIgnoreCase("integer")) {
			return Column.DataTypeEnum.INT;
		}
		if(type.equalsIgnoreCase("string")) {
			return Column.DataTypeEnum.STRING;
		}
		if(type.equalsIgnoreCase("number")) {
			return Column.DataTypeEnum.NUMBER;
		}
		if(type.equalsIgnoreCase("boolean")) {
			return Column.DataTypeEnum.BOOLEAN;
		}
		if(type.equalsIgnoreCase("date")) {
			return Column.DataTypeEnum.DATE;
		}
		if(type.equalsIgnoreCase("time")) {
			return Column.DataTypeEnum.TIME;
		}
		if(type.equalsIgnoreCase("datetime")) {
			return Column.DataTypeEnum.DATETIME;
		}
		if(type.equalsIgnoreCase("object")) {
			return Column.DataTypeEnum.BLOB;
		}
		if(type.equalsIgnoreCase("array")) {
			return Column.DataTypeEnum.ARRAY;
		}
		if(type.equalsIgnoreCase("year")) {
			return Column.DataTypeEnum.YEAR;
		}
		if(type.equalsIgnoreCase("yearmonth")) {
			return Column.DataTypeEnum.DATE;
		}
//		if(type.equalsIgnoreCase("duration")) {
//			return Column.DataTypeEnum.;
//		}
		if(type.equalsIgnoreCase("geopoint")) {
			return Column.DataTypeEnum.POINT;
		}
		if(type.equalsIgnoreCase("geojson")) {
			return Column.DataTypeEnum.JSON;
		}
//		if(type.equalsIgnoreCase("any")) {
//			return Column.DataTypeEnum.VARIANT;
//		}
		return Column.DataTypeEnum.UNKNOWN;
	}
}
