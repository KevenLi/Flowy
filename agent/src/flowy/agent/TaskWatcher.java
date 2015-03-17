package flowy.agent;

import java.util.List;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.AsyncCallback.Children2Callback;
import org.apache.zookeeper.data.Stat;

public class TaskWatcher implements Watcher, Children2Callback {

	ZooKeeper zk;
	String rootNode;
	String tasksNode;
	TaskWatcherListener listener;
	Watcher chainedWatcher;
	boolean dead;
	List<String> tasks;

	public TaskWatcher(ZooKeeper zk, String rootNode, Watcher chainedWatcher,
			TaskWatcherListener listener) {
		this.zk = zk;
		this.rootNode = rootNode;
		this.tasksNode = rootNode + "/tasks";
		this.listener = listener;
		this.chainedWatcher = chainedWatcher;

		zk.getChildren(tasksNode, this, this, null);
	}

	public void process(WatchedEvent event) {
		String path = event.getPath();
		EventType type = event.getType();
		if (path != null && path.equals(tasksNode)
				&& type == EventType.NodeChildrenChanged) {
			// children changed, get result and rewatch it
			zk.getChildren(tasksNode, this, this, null);
		}
		if (chainedWatcher != null) {
			chainedWatcher.process(event);
		}
	}

	public void processResult(int rc, String path, Object ctx,
			List<String> children, Stat stat) {
		KeeperException.Code code = KeeperException.Code.get(rc);

		if (code == KeeperException.Code.OK) {
			if (tasks == null) {
				tasks = children;
				zk.getChildren(path, this, this, null);
				return;
			}

			for (String task : children) {
				if (!tasks.contains(task)) {
					listener.onTaskCreated(task);
				}
			}

			for (String oldTask : tasks) {
				if (!children.contains(oldTask)) {
					listener.onTaskDeleted(oldTask);
				}
			}
			tasks = children;
		}
	}

	public interface TaskWatcherListener {

		void onTaskCreated(String name);

		void onTaskDeleted(String name);

		void onTaskChanged();

		void onClose(KeeperException.Code rc);
	}

}