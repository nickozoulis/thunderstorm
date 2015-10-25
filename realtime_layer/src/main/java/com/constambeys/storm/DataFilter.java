package com.constambeys.storm;

import java.io.Serializable;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;

public class DataFilter implements Serializable {

	transient ScriptEngine engine;
	Pattern pattern1 = Pattern.compile("\\{([0-9])+\\}");
	String expr1;
	String expr2;
	String op;

	public DataFilter(String expr1, String op, String expr2) {
		this.expr1 = expr1;
		this.expr2 = expr2;
		this.op = op;
	}

	public boolean run(Point p) throws ScriptException {

		if (engine == null) {
			ScriptEngineManager mgr = new ScriptEngineManager();
			engine = mgr.getEngineByName("JavaScript");
		}
		Object aa = engine.eval(substitute(expr1, p));
		Object bb = engine.eval(substitute(expr2, p));

		Double a, b;
		if (aa.getClass() == Integer.class) {
			a = ((Integer) aa).doubleValue();
		} else {
			a = (Double) aa;
		}

		if (bb.getClass() == Integer.class) {
			b = ((Integer) bb).doubleValue();
		} else {
			b = (Double) bb;
		}

		if (op.equals("<")) {
			if (a < b)
				return true;
			else
				return false;
		}

		return false;
	}

	private String substitute(String expr, Point p) {
		Matcher matcher1 = pattern1.matcher(expr);
		while (matcher1.find()) {
			int number = Integer.parseInt(matcher1.group(1));
			expr = matcher1.replaceFirst(Double.toString(p.components[number]));
			matcher1 = pattern1.matcher(expr);
		}
		return expr;
	}

	@Override
	public String toString() {
		return expr1 + " " + op + " " + expr2;
	}

}
