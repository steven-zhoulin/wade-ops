package com.wade.ops.harmonius;

import java.text.SimpleDateFormat;
import java.util.*;

public class Test {
	public static void main(String[] args) throws Exception {
		System.out.println("------------------------------------");
		SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		Date date = format.parse("1508740204107");
		System.out.println(date.getTime());
	}
}
