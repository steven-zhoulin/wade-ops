package com.wade.ops.harmonius;

import com.wade.ops.util.TripleDES;

import java.security.SecureRandom;
import java.util.*;

public class Test {
	public static void main(String[] args) throws Exception {
		/*
		System.out.println(System.currentTimeMillis());
		Date date = new Date(Long.parseLong("1512551373485"));
		System.out.println(date);
		*/

		System.out.println(TripleDES.decrypt("3Hm0ks1J9Fs+6JKhtwDcZQ=="));
	}
}
