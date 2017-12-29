package com.wade.ops.harmonius;

import com.wade.ops.util.TripleDES;
import org.apache.commons.lang3.StringUtils;

import java.security.SecureRandom;
import java.util.*;

public class Test {
	public static void main(String[] args) throws Exception {

		System.out.println(System.currentTimeMillis());
		Date date = new Date(Long.parseLong("1514292160861"));
		System.out.println(date);


		/*
		System.out.println(TripleDES.decrypt("3Hm0ks1J9Fs+6JKhtwDcZQ==")); // resource, update, 7R72!cS4
		System.out.println(StringUtils.isNotBlank(null));
		System.out.println(new HashMap<>().get("xn"));
		*/

		List<Integer> rtn = new ArrayList<>();
		for (int i = 0; i < 100; i++) {
			rtn.add(i);
		}
		System.out.println(rtn.subList(0, 50));
	}
}
