package eu.fbk.dslab.digitalhub.openmetadata.connector.parser;

import java.util.ArrayList;
import java.util.List;

import org.openmetadata.client.model.Column;

public class TableColumn {
	private String name;
	private Column.DataTypeEnum type;
	private Column.ConstraintEnum constraint;
	private List<String> preview = new ArrayList<>();
	
	public TableColumn() {}
	
	public TableColumn(String name, Column.DataTypeEnum type) {
		this.name = name;
		this.type = type;
	}
	
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public Column.DataTypeEnum getType() {
		return type;
	}
	public void setType(Column.DataTypeEnum type) {
		this.type = type;
	}
	public Column.ConstraintEnum getConstraint() {
		return constraint;
	}
	public void setConstraint(Column.ConstraintEnum constraint) {
		this.constraint = constraint;
	}
	public List<String> getPreview() {
		return preview;
	}
	public void setPreview(List<String> preview) {
		this.preview = preview;
	}
}
