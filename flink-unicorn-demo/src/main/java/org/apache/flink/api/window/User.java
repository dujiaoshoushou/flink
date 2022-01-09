package org.apache.flink.api.window;

/**
 * Created with IntelliJ IDEA.
 * User: unicorn
 * Date: 2021/12/23 5:47 下午
 * Description: No Description
 */
public class User {
	private String name ;
	private Integer age;

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public Integer getAge() {
		return age;
	}

	public void setAge(Integer age) {
		this.age = age;
	}

	public User(String name, Integer age) {
		this.name = name;
		this.age = age;
	}
}
