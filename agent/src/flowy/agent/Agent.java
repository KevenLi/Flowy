package flowy.agent;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.List;
import java.util.Properties;

import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooKeeper.States;
import org.apache.zookeeper.data.Stat;

import sun.management.VMManagement;

public class Agent implements Runnable, AgentTasksWatcher.Listener,
		RuntimeStarter.RuntimeStarterCallback {
	final String TASKSNODE = "%s/tasks";
	final String TASKSERVERSNODE = "%s/taskservers";
	final String CURRENTTSNODE = "%s/taskservers/%s";
	final String TASKSTATUS = "%s/tasks/tasksstatus";
	final String TASKSERVERAGENTNODE = "%s/taskservers/%s/agent";
	final String CURRENTTSTASKSNODE = "%s/taskservers/%s/tasks";
	
	
	static final int TASK_ERROR_CODE_RBMQ = 81;
	static final int TASK_ERROR_CODE_CASSANDRA = 82;
	static final int TASK_ERROR_CODE_OS_ENVIRONMENT = 83;
	static final int TASK_ERROR_CODE_UNKOWN = 100;
	static final int TASK_ERROR_CODE_NORMAL = 0;
	static final int TASK_ERROR_CODE_FORCE_KILL = 137;
	static final int TASK_ERROR_CODE_KILL = 143;
	static final int TASK_ERROR_CODE_INVALID = -1;

	private static final HashMap<Integer, String> TASK_ERROR_STRs;
	static {
		TASK_ERROR_STRs = new HashMap<Integer, String>();
		TASK_ERROR_STRs.put(TASK_ERROR_CODE_RBMQ, "rabbitmq连接错误");
		TASK_ERROR_STRs.put(TASK_ERROR_CODE_CASSANDRA, "cassandra异常");
		TASK_ERROR_STRs.put(TASK_ERROR_CODE_OS_ENVIRONMENT, "操作系统环境异常");
		TASK_ERROR_STRs.put(TASK_ERROR_CODE_UNKOWN, "未知异常");		
		TASK_ERROR_STRs.put(TASK_ERROR_CODE_NORMAL, "正常");	
		TASK_ERROR_STRs.put(TASK_ERROR_CODE_FORCE_KILL, "强退, kill -9");
		TASK_ERROR_STRs.put(TASK_ERROR_CODE_KILL, "agent销毁task");
		TASK_ERROR_STRs.put(TASK_ERROR_CODE_INVALID, "非法错误");
	}

	static Logger logger = Logger.getLogger(Agent.class);

	Integer agentProcessPid = -1;
	
	ZooKeeper zk;
	List<String> tasks = new ArrayList<String>();

	// zookeeper nodes
	String flowyRoot;
	String tasksNode;
	String computeServersNode;
	String currentTsAgentNode;
	String currentTsNode;
	String currentTsTasksNode;
	String taskStatus;

	Hashtable<String, RuntimeStarter> processes = new Hashtable<String, RuntimeStarter>();

	TaskWatcher taskWatcher;
	AgentTasksWatcher agentTasksWatcher;
	boolean dead = false;
	
    public Agent(String flowyRoot, ZooKeeper zk, String computerServerName)
			throws IOException, KeeperException, InterruptedException {
		// nodes naming
		this.flowyRoot = flowyRoot;
		this.zk = zk;
		tasksNode = String.format(TASKSNODE, flowyRoot);
		taskStatus = String.format(TASKSTATUS, flowyRoot);
		computeServersNode = String.format(TASKSERVERSNODE, flowyRoot);

		currentTsNode = String.format(CURRENTTSNODE, flowyRoot,
				computerServerName);
		currentTsAgentNode = String.format(TASKSERVERAGENTNODE, flowyRoot,
				computerServerName);
		currentTsTasksNode = String.format(CURRENTTSTASKSNODE, flowyRoot,
				computerServerName);

		// create default nodes
		Stat s = zk.exists(flowyRoot, false);
		if (s == null) {
			zk.create(flowyRoot, new byte[0], Ids.OPEN_ACL_UNSAFE,
					CreateMode.PERSISTENT);
		}

		s = zk.exists(tasksNode, false);
		if (s == null) {
			zk.create(tasksNode, new byte[0], Ids.OPEN_ACL_UNSAFE,
					CreateMode.PERSISTENT);
		}
		s = zk.exists(taskStatus, false);
		if (s == null) {
			zk.create(taskStatus, new byte[0], Ids.OPEN_ACL_UNSAFE,
					CreateMode.PERSISTENT);
		}		

		s = zk.exists(computeServersNode, false);
		if (s == null) {
			zk.create(computeServersNode, new byte[0], Ids.OPEN_ACL_UNSAFE,
					CreateMode.PERSISTENT);
		}
		
		s = zk.exists(currentTsNode, false);
		if (s == null) {
			zk.create(currentTsNode, new byte[0], Ids.OPEN_ACL_UNSAFE,
					CreateMode.PERSISTENT);
		}

		s = zk.exists(currentTsTasksNode, false);
		if (s == null) {
			zk.create(currentTsTasksNode, new byte[0], Ids.OPEN_ACL_UNSAFE,
					CreateMode.PERSISTENT);
		}

		s = zk.exists(currentTsAgentNode, false);
		// register current agent, multiple agents on the same machine would
		// fail.
		if (s == null) {
			zk.create(currentTsAgentNode, new byte[0], Ids.OPEN_ACL_UNSAFE,
					CreateMode.EPHEMERAL);
		} else {
			System.err
					.format("Agent for computeServer : %s already started.\r\n Exiting ... \r\n",
							computerServerName);
			logger.error(String.format("Agent for computeServer : %s already started.\r\n Exiting ... \r\n",
							computerServerName));
			System.exit(2);
		}

		

		agentTasksWatcher = new AgentTasksWatcher(zk, currentTsTasksNode, null,
				this);
	}

	@Override
	public void onTaskCreated(String name) {
		logger.info("onTaskCreated : name = " + name);
		String node = currentTsTasksNode + "/" + name;
		Stat stat = new Stat();
		try {
			byte[] taskData = zk.getData(node, false, stat);
			Properties config = new Properties();
			InputStream inStream = new ByteArrayInputStream(taskData);
			config.load(inStream);
			String runtime_type = config
					.getProperty(RuntimeStarter.CONFIG_RUNTIME_TYPE);
			switch (runtime_type) {
			case "python":
				createTaskZKStatusNode(name);				
				RuntimeStarter starter = new PythonRuntimeStarter();
				starter.init(name, config, this.getAgentProcessId());
				starter.start(this);
				String nodedata = Integer.toString(starter.getPid());
				zk.create(node + "/running", nodedata.getBytes(), Ids.OPEN_ACL_UNSAFE,
						CreateMode.EPHEMERAL);
				
				String pathNode = node + "/path";
				if (zk.exists(pathNode, false) != null){
					zk.delete(pathNode, -1);
				}
				zk.create(pathNode, starter.getRunningPath().getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
				processes.put(name, starter);
				logger.info(String.format("Process started, pid:%d", starter.getPid()));
				break;
			case "lua":
				throw new Exception("Lua runtime has not been supported");
			default:
				throw new Exception("Unknown runtime type" + runtime_type);
			}

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

	}

	/**
	 * @param name task name
	 */
	private void createTaskZKStatusNode(String name) {
		String node_status_node = taskStatus + "/" + name;
		String node_status_node_data = TASK_ERROR_STRs.get(TASK_ERROR_CODE_NORMAL);
		try {
			if (zk.exists(node_status_node, false) != null){
				zk.delete(node_status_node, -1);
			}
			
			zk.create(node_status_node, node_status_node_data.getBytes(), Ids.OPEN_ACL_UNSAFE,
					CreateMode.EPHEMERAL);
			logger.info("Task " + name + "'s status is normal\r\n");
			
		} catch (KeeperException | InterruptedException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
	}

	@Override
	public void onTaskDeleted(String name) {
		logger.info("onTaskDeleted : name = " + name);
		synchronized (processes) {
			if (processes.containsKey(name)) {
				processes.get(name).Stop();
			}
		}
	}

	@Override
	public void onTaskChanged() {
		logger.debug("onTaskChanged");
	}

	@Override
	public void onClose(KeeperException.Code code) {
		logger.debug("onClose");
		synchronized (this) {
			notifyAll();
		}
	}

	public void run() {
		try {
			logger.info("Agent started.");
			
			String[] tasks = agentTasksWatcher.getTasks();
			for(String task: tasks){
				this.onTaskCreated(task);
			}
			synchronized (this) {
				while (!agentTasksWatcher.dead && !dead) {
					wait();
				}
				dead = true;
			}
		} catch (InterruptedException e) {
			logger.info("Agent Exiting...");
		}
	}

	public void stop() {
		logger.info("Agent stopping");
		synchronized (this) {
			for(RuntimeStarter starter : this.processes.values()){
				starter.Stop();
			}
			dead = true;
			notifyAll();
		}

	}

	@Override
	public void onProcessStoped(String name, int exitValue) {
		System.out.format("Process %s stoped %d", name, exitValue);
		String node = currentTsTasksNode + "/" + name;
		String running_node = node + "/running";
		try {
			if (zk.getState() == States.CONNECTED && zk.exists(running_node, false) != null){
				zk.delete(running_node, -1);
			}
		} catch (InterruptedException | KeeperException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
		updateTaskZKStatusNodeData(name, exitValue);
		
	}

	/**
	 * @param name, task name
	 * @param exitValue, task exit code
	 */
	private void updateTaskZKStatusNodeData(String name, int exitValue) {
		String node_status_node = taskStatus + "/" + name;
		String node_status_node_data = "";
		switch(exitValue){
		case TASK_ERROR_CODE_RBMQ: 
			node_status_node_data = TASK_ERROR_STRs.get(exitValue);break;
		case TASK_ERROR_CODE_CASSANDRA: 
			node_status_node_data = TASK_ERROR_STRs.get(exitValue);break;
		case TASK_ERROR_CODE_OS_ENVIRONMENT: 
			node_status_node_data = TASK_ERROR_STRs.get(exitValue);break;
		case TASK_ERROR_CODE_UNKOWN: 
			node_status_node_data = TASK_ERROR_STRs.get(exitValue);break;
		case TASK_ERROR_CODE_NORMAL: 
			node_status_node_data = TASK_ERROR_STRs.get(exitValue);break;
		case TASK_ERROR_CODE_KILL:
			node_status_node_data = TASK_ERROR_STRs.get(exitValue);break;
		case TASK_ERROR_CODE_FORCE_KILL: 
			node_status_node_data = TASK_ERROR_STRs.get(exitValue);break;
		default:
			 node_status_node_data = TASK_ERROR_STRs.get(TASK_ERROR_CODE_INVALID);break;
			
		}
		logger.info("Task " + name + "exitcode" + exitValue + ", exit code meaning:" 
				+ node_status_node_data);
		try {
			if (zk.exists(node_status_node, false) != null){
				zk.setData(node_status_node, node_status_node_data.getBytes(), -1);
				System.out.format("Task %s's status is %s\r\n", name, node_status_node_data);	
			}
						
		} catch (KeeperException | InterruptedException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
	}
	
	public void onTaskStartCommand(String name){
		System.out.format("Process start command %s", name);
		String node = currentTsTasksNode + "/" + name;
		try {
			if (zk.exists(node + "/running", false) != null){
				System.out.format("Task %s is running \r\n", name);
				return;
			}
		} catch (KeeperException | InterruptedException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		createTaskZKStatusNode(name);
		RuntimeStarter starter = this.processes.get(name);
		starter.start(this);
		String nodedata = Integer.toString(starter.getPid());
		
		try {
			zk.create(node + "/running", nodedata.getBytes(), Ids.OPEN_ACL_UNSAFE,
					CreateMode.EPHEMERAL);
			String pathNode = node + "/path";
			if (zk.exists(pathNode, false) != null){
				zk.delete(pathNode, -1);
			}
			zk.create(pathNode, starter.getRunningPath().getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
		} catch (KeeperException | InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
	}


	public Integer getAgentProcessId() {
	
		if (agentProcessPid == -1) {
			try {
				RuntimeMXBean runtimeMXBean = ManagementFactory
						.getRuntimeMXBean();
				Field jvmField = runtimeMXBean.getClass().getDeclaredField(
						"jvm");
				jvmField.setAccessible(true);
				VMManagement vmManagement = (VMManagement) jvmField
						.get(runtimeMXBean);
				Method getProcessIdMethod = vmManagement.getClass()
						.getDeclaredMethod("getProcessId");
				getProcessIdMethod.setAccessible(true);
				agentProcessPid = (Integer) getProcessIdMethod
						.invoke(vmManagement);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		return agentProcessPid;
	}
	public Agent(){}
	public static void main(String[] args) {
		Agent a = new Agent();
		System.out.print(a.getAgentProcessId());
	}
}