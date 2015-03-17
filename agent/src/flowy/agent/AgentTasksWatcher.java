package flowy.agent;

import java.util.List;

import org.apache.zookeeper.AsyncCallback.Children2Callback;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

public class AgentTasksWatcher implements Watcher, Children2Callback {

	ZooKeeper zk;
	String tasksNode;
	Listener listener;
	Watcher chainedWatcher;
	boolean dead;
	List<String> tasks;

	public AgentTasksWatcher(ZooKeeper zk, String tsNode,
			Watcher chainedWatcher, Listener listener) throws KeeperException,
			InterruptedException {
		this.zk = zk;
		// this.tasksNode = tsNode + "/tasks";
		this.tasksNode = tsNode;
		this.listener = listener;
		this.chainedWatcher = chainedWatcher;

		this.tasks = zk.getChildren(tasksNode, this);
		for (String task : this.tasks) {
			zk.exists(tasksNode + "/" + task + "/start", this);
		}
	}

	public void process(WatchedEvent event) {
		String path = event.getPath();
		KeeperState state = event.getState();
		EventType type = event.getType();
		System.out.format("%s\t%s\t%s\r\n", path, state, type);
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
				dead = true;
				listener.onClose(KeeperException.Code.SESSIONEXPIRED);
				break;
			default:
				break;
			}
		} else if (path != null && path.equals(tasksNode)
				&& type == EventType.NodeChildrenChanged) {
			// Something has changed on the node, let's find out
			zk.getChildren(tasksNode, this, this, null);
		} else if (path != null && path.startsWith(tasksNode + "/")
				&& path.endsWith("/start")) {
			if (type == EventType.NodeCreated) {
				String taskNode = path.substring(0, path.length() - 6);
				String taskName = taskNode.substring(taskNode.lastIndexOf("/")+1);
				this.listener.onTaskStartCommand(taskName);
			}
			try {
				zk.exists(path, this);
			} catch (KeeperException | InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		if (chainedWatcher != null) {
			chainedWatcher.process(event);
		}
	}

	public void processResult(int rc, String path, Object ctx,
			List<String> children, Stat stat) {
		boolean exists;
		KeeperException.Code code = KeeperException.Code.get(rc);
		switch (code) {
		case OK:
			exists = true;
			break;
		case NONODE:
			exists = false;
			break;
		case SESSIONEXPIRED:
		case NOAUTH:
			dead = true;
			listener.onClose(code);
			return;
		default:
			// Retry errors
			zk.getChildren(tasksNode, this, this, null);
			return;
		}

		if (exists) {
			if (tasks == null) {
				tasks = children;
				zk.getChildren(path, this, this, null);
				return;
			}

			synchronized (tasks) {
				for (String task : children) {
					if (!tasks.contains(task)) {
						listener.onTaskCreated(task);
						//tasks.add(task);
					}
				}
				
				for (String oldTask : tasks) {
					if (!children.contains(oldTask)) {
						listener.onTaskDeleted(oldTask);
						//tasks.remove(oldTask);
					}
				}
				tasks = children;
			}

			
		}
	}

	public String[] getTasks() {
		return this.tasks.toArray(new String[0]);
	}

	public interface Listener {

		void onTaskCreated(String name);

		void onTaskDeleted(String name);

		void onTaskChanged();

		void onClose(KeeperException.Code rc);

		void onTaskStartCommand(String name);
	}

}