package cn.edu.nupt.hadoop.rpc;

public interface LoginServiceInterface {
	public static final long versionID=1l;
	public String login(String username,String passwd);
}
