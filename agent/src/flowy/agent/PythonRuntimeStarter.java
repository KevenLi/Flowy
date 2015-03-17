package flowy.agent;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.lang.ProcessBuilder.Redirect;
import java.lang.reflect.Field;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Properties;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;
import org.apache.zookeeper.KeeperException;

import com.sun.jna.Pointer;

import flowy.agent.win32.Kernel32;
import flowy.agent.win32.W32API;

public class PythonRuntimeStarter implements RuntimeStarter {

	private String name;
	private Properties config;
	private Process process;
	private RuntimeStarterCallback _callback;
	private Thread waitingThread;
	private File runningPath;
	private Integer parentProcessPid;
	
	private static Logger logger = Logger.getLogger(PythonRuntimeStarter.class);

	@Override
	public void init(String name, Properties config, Integer parentProcessPid) {
		this.name = name;
		this.config = config;
		this.parentProcessPid = parentProcessPid;
	}

	public int getPid() {
		int pid;
		if (process.getClass().getName().equals("java.lang.UNIXProcess")) {
			/* get the PID on unix/linux systems */
			try {
				Field f = process.getClass().getDeclaredField("pid");
				f.setAccessible(true);
				pid = f.getInt(process);
				return pid;
			} catch (Throwable e) {
			}
		}

		if (process.getClass().getName().equals("java.lang.Win32Process")
				|| process.getClass().getName().equals("java.lang.ProcessImpl")) {
			/* determine the pid on windows plattforms */
			try {
				Field f = process.getClass().getDeclaredField("handle");
				f.setAccessible(true);
				long handl = f.getLong(process);

				Kernel32 kernel = Kernel32.INSTANCE;
				W32API.HANDLE handle = new W32API.HANDLE();
				handle.setPointer(Pointer.createConstant(handl));
				pid = kernel.GetProcessId(handle);
				return pid;
			} catch (Throwable e) {
			}
		}
		return 0;

	}
	
	public String getRunningPath(){
		return runningPath.getAbsolutePath();
	}

	@Override
	public Process start(RuntimeStarterCallback callback) {
		this._callback = callback;

		String package_url = config.getProperty(CONFIG_PACKAGE_URL);
		String runtime_config_url = config
				.getProperty(CONFIG_RUNTIME_CONFIG_URL);
		String task_config_url = config.getProperty(CONFIG_TASK_CONFIG_URL);
		String runtime_type = config.getProperty(CONFIG_RUNTIME_TYPE);

		String taskRunDir = "./tasks/" + name + "/";
		File dir = new File(taskRunDir);
		runningPath = dir; 
		if (!dir.exists()) {
			dir.mkdirs();
		}
		try {
			if (package_url != null) {
				String package_local_file_path = taskRunDir + "/package.zip";
				try {
					org.apache.commons.io.FileUtils.copyURLToFile(new URL(
							package_url), new File(package_local_file_path));
				} catch (MalformedURLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				unzip(package_local_file_path, taskRunDir);
			}

			if (runtime_config_url != null) {
				String runtime_config_local_file_path = taskRunDir
						+ "/runtime.config";
				FileUtils.copyURLToFile(new URL(runtime_config_url), new File(
						runtime_config_local_file_path));
			}

			if (task_config_url != null) {
				String task_config_local_file_path = taskRunDir
						+ "/task.config";
				FileUtils.copyURLToFile(new URL(task_config_url), new File(
						task_config_local_file_path));
			}

			if (runtime_type == null) {
				throw new Exception("runtime type cannot be null.");
			}

			File runtimeFile = new File(taskRunDir , "TaskRuntime.py");
			logger.info(runtimeFile.getCanonicalPath());
			ProcessBuilder pb = new ProcessBuilder("python", "TaskRuntime.py", ""+this.parentProcessPid);
			pb.directory(new File(taskRunDir));
			File log = new File(taskRunDir, "process.log");
			File errorlog = new File(taskRunDir, "error.log");
			pb.redirectErrorStream(true);
			pb.redirectError(Redirect.appendTo(errorlog));
			pb.redirectOutput(Redirect.appendTo(log));
			process = pb.start();

			waitingThread = new Thread(new Runnable() {
				@Override
				public void run() {
					try {
						process.waitFor();
						_callback.onProcessStoped(name, process.exitValue());
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}

				}
			});
			waitingThread.start();
		} catch (KeeperException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}

	private void unzip(String zipfile, String dir) {
		try {
			ZipInputStream Zin = new ZipInputStream(
					new FileInputStream(zipfile));
			BufferedInputStream Bin = new BufferedInputStream(Zin);
			String Parent = dir;
			File Fout = null;
			ZipEntry entry;
			try {
				while ((entry = Zin.getNextEntry()) != null) {
					if (entry.isDirectory()) {
						continue;
					}
					Fout = new File(Parent, entry.getName());
					if (!Fout.exists()) {
						(new File(Fout.getParent())).mkdirs();
					}
					FileOutputStream out = new FileOutputStream(Fout);
					BufferedOutputStream Bout = new BufferedOutputStream(out);
					int b;
					while ((b = Bin.read()) != -1) {
						Bout.write(b);
					}
					Bout.close();
					out.close();
				}
				Bin.close();
				Zin.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Override
	public void Stop() {
		logger.info("killing process");
		process.destroy();
	}
}
