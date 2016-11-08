package com.krishagni.query.domain;

import java.util.List;

public class SelectField {
	public static class Function {
		String name;

		String desc;

		public String getName() {
			return name;
		}

		public void setName(String name) {
			this.name = name;
		}

		public String getDesc() {
			return desc;
		}

		public void setDesc(String desc) {
			this.desc = desc;
		}
	};

	private String name;

	private List<Function> aggFns;

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public List<Function> getAggFns() {
		return aggFns;
	}

	public void setAggFns(List<Function> aggFns) {
		this.aggFns = aggFns;
	}
}