package flowy.agent;

import org.apache.zookeeper.AsyncCallback.StatCallback;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

public class Supervisor implements Runnable, SupervisorWatcher.Listener, Watcher, StatCallback {

	static final String SUPERVISORNODE = "%s/supervisor";

	ZooKeeper zk;
	SupervisorWatcher supervisorWatcher;
	boolean isActive = false;
	Thread runThread;
	String flowyRoot;
	String supervisorNode;

	public boolean dead;

	public Supervisor(String flowyRoot,ZooKeeper zk) {
		this.zk = zk;
		this.flowyRoot = flowyRoot;
		this.supervisorNode = String.format(SUPERVISORNODE, flowyRoot);
		
	}

	@Override
	public void run() {
		synchronized (this) {
			supervisorWatcher = new SupervisorWatcher("/flowy", zk, this);
			zk.exists(supervisorNode, this, this, null);
			while (!dead) {
				try {
					wait();
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		}
	}

	@Override
	public void onClose(Code code) {
		stop();

	}

	public void stop() {
		synchronized (this) {
			dead = true;
			ThreadGroup group = Thread.currentThread().getThreadGroup();
			group.notifyAll();
		}
	}

	@Override
	public void onLeaderRemoved() {
		System.out.println("Leader remeved");
	}

	public void start() {
		synchronized (this) {
			isActive = true;
			runThread = new Thread(this);
			runThread.start();
		}
	}

	@Override
	public void processResult(int rc, String path, Object ctx, Stat stat) {
		KeeperException.Code code = KeeperException.Code.get(rc);
		if (path.equals(supervisorNode)){
			if(code == Code.NONODE){
				active();
			}
			
			
		}
	}

	@Override
	public void process(WatchedEvent event) {
		if (event.getPath().equals(supervisorNode)){
			zk.exists(supervisorNode, this, this, null);
		}
	}
	
	// try to active the supervisor
	private void active(){
		synchronized (this) {
			if (!isActive){
				try {
					zk.create(supervisorNode, new byte[0], Ids.OPEN_ACL_UNSAFE,  CreateMode.EPHEMERAL);
					isActive = true;
					System.out.println("running as supervisor.");
				} catch (KeeperException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
	}

}
