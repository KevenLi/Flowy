package flowy.agent;

import java.util.Properties;

public interface RuntimeStarter {

	final String CONFIG_PACKAGE_URL = "package_url";
	final String CONFIG_RUNTIME_CONFIG_URL = "runtime_config_url";
	final String CONFIG_TASK_CONFIG_URL = "task_config_url";
	final String CONFIG_RUNTIME_TYPE = "runtime_type";

	void init(String name, Properties config, Integer parentProcessPid);

	Process start(RuntimeStarterCallback callback);

	void Stop();
	
	int getPid();
	
	String getRunningPath();

	public interface RuntimeStarterCallback {
		void onProcessStoped(String name, int exitValue);
	}
}
