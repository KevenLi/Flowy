package flowy.agent;

import java.util.List;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.AsyncCallback.Children2Callback;
import org.apache.zookeeper.data.Stat;

public class TaskInstanceWatcher implements Watcher, Children2Callback {

	ZooKeeper zk;
	String rootNode;
	String tasksNode;
	String taskNode;
	String taskInstanceNode;
	TaskInstanceWatcherListener listener;
	Watcher chainedWatcher;
	boolean dead;
	List<String> instances;

	public TaskInstanceWatcher(ZooKeeper zk, String rootNode, String taskName,
			Watcher chainedWatcher, TaskInstanceWatcherListener listener) {
		this.zk = zk;
		this.rootNode = rootNode;
		this.tasksNode = rootNode + "/tasks";
		this.taskNode = this.tasksNode + "/" + taskName;
		this.taskInstanceNode = this.taskNode + "/instances";
		this.listener = listener;
		this.chainedWatcher = chainedWatcher;

		zk.getChildren(taskInstanceNode, true, this, null);
	}

	public void process(WatchedEvent event) {
		String path = event.getPath();
		if (event.getType() == Event.EventType.None) {
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
		} else {
			if (path != null && path.equals(taskInstanceNode)) {
				// Something has changed on the node, let's find out
				zk.getChildren(taskInstanceNode, true, this, null);
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
			zk.getChildren(taskInstanceNode, true, this, null);
			return;
		}

		if (exists) {
			if (instances == null) {
				instances = children;
				zk.getChildren(taskInstanceNode, true, this, null);
				return;
			}

			for (String instance : children) {
				if (!instances.contains(instance)) {
					listener.onTaskInstanceCreated(instance);
				}
			}

			for (String instance : instances) {
				if (!children.contains(instance)) {
					listener.onTaskInstanceDeleted(instance);
				}
			}

			instances = children;
			zk.getChildren(taskInstanceNode, true, this, null);
		}
	}

	public interface TaskInstanceWatcherListener {

		void onTaskInstanceCreated(String name);

		void onTaskInstanceDeleted(String name);

		void onClose(KeeperException.Code rc);
	}

}