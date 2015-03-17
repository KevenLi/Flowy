package flowy.agent;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.apache.commons.codec.binary.Hex;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooKeeper;

public class Shell implements Watcher {

	final String FLOWYROOT = "/flowy";

	public static void main(String[] args) throws IOException, KeeperException,
			InterruptedException {
		if (args.length < 1) {
			showUsage();
			System.exit(2);
		}

		Properties prop = new Properties();
		try {
			InputStream in = new FileInputStream(args[0]);
			prop.load(in);
		} catch (FileNotFoundException ex) {
			System.out.println("Cannot load file " + args[0]);
			System.err.println("Cannot load file " + args[0]);
			return;
		} catch (IOException ex) {
			ex.printStackTrace();
		}

		PropertyConfigurator.configure("../conf/log4j.properties");

		String hosts = prop.getProperty("zookeeper.hosts");
		String tsName = prop.getProperty("nodename");

		boolean runAgent = true;
		boolean runSupervisor = false;

		if (!runAgent && !runSupervisor) {
			System.err
					.println("Either run as agent or run as supervisor is needed.");
			showUsage();
			System.exit(2);
		}
		Shell shell = new Shell(hosts, tsName, runAgent, runSupervisor);
		shell.run();
	}

	public static void showUsage() {
		System.err.println("USAGE: Shell conf_file");
	}

	Logger logger = Logger.getLogger(getClass());

	Agent agent;
	Supervisor supervisor;

	// thread which agent is running on.
	Thread agentThread;

	// thread which supervisor is running on.
	Thread supervisorThread;

	// global zookeeper.
	ZooKeeper zk;
	
	String hosts;
	String tsname;
	
	

	public Shell(String hosts, String tsName, boolean runAgent,
			boolean runSupervisor) throws IOException, KeeperException,
			InterruptedException {
		String flowyRoot = FLOWYROOT;
		this.tsname = tsName;
		this.hosts = hosts;
		logger.debug(String.format("starting shell, flowyRoot : %s", flowyRoot));
		zk = new ZooKeeper(hosts, 3000, this);
		if (runAgent) {
			agent = new Agent(flowyRoot, zk, tsName);
		}

		if (runSupervisor) {
			supervisor = new Supervisor(flowyRoot, zk);
		}
	}

	public void run() {
		if (agentThread == null && agent != null) {
			agentThread = new Thread(agent);
			agentThread.start();
		}

		if (supervisorThread == null && supervisor != null) {
			supervisorThread = new Thread(supervisor);
			supervisorThread.start();
		}

		ThreadGroup group = Thread.currentThread().getThreadGroup();
		synchronized (group) {
			while (!((supervisor != null && supervisor.dead) || (agent != null && agent.dead))) {
				try {
					group.wait();
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
			stopAddClearup();
		}
	}

	@Override
	public void process(WatchedEvent event) {
		String path = event.getPath();
		KeeperState state = event.getState();
		EventType type = event.getType();
		logger.debug(String.format("Shell.process %s\t%s\t%s\r\n", path, state,
				type));
		if (type == Event.EventType.None) {
			logger.debug(String.format(
					"New session establised, sessionid:%s, sessionpwd:%s",
					zk.getSessionId(), Hex.encodeHexString(zk.getSessionPasswd()) ));
		}
		if (type == Event.EventType.None) {
			// We are are being told that the state of the
			// connection has changed
			switch (event.getState()) {
			case SyncConnected:
				// In this particular example we don't need to do anything
				// here - watches are automatically re-registered with
				// server and any watches triggered while the client was
				// disconnected will be delivered (in order of course)
				break;
			case Expired:
				// It's all over
				//stopAddClearup();
				agent.stop();
				try {
					zk = new ZooKeeper(hosts, 1000, this);
					agent = new Agent(FLOWYROOT, zk, tsname);
					agentThread.join();
					agentThread = new Thread(agent);
					agentThread.start();
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (KeeperException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				
				break;
			default:
				break;
			}
		}
	}

	// stop the all shell, clearup all threads, zk instance.
	public void stopAddClearup() {
		logger.debug("Shell.stopAndClearup");
		if (agent != null) {
			agent.stop();
		}
		if (supervisor != null) {
			supervisor.stop();
		}
		ThreadGroup group = Thread.currentThread().getThreadGroup();
		group.notifyAll();
	}

}
