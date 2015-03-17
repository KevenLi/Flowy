package flowy.agent;

import org.apache.zookeeper.AsyncCallback.StatCallback;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.data.Stat;

public class SupervisorWatcher implements Watcher, StatCallback {

	boolean dead = false;
	Listener listener;
	String leaderAgentNode;
	ZooKeeper zk;

	public SupervisorWatcher(String flowyRoot, ZooKeeper zk, Listener listener) {
		leaderAgentNode = flowyRoot + "/leaderagent";
		this.listener = listener;
		this.zk = zk;
		zk.exists(leaderAgentNode, this, this, null);
	}

	interface Listener {
		void onClose(KeeperException.Code code);

		void onLeaderRemoved();
	}

	@Override
	public void processResult(int rc, String path, Object ctx, Stat stat) {
		KeeperException.Code code = KeeperException.Code.get(rc);
		switch (code) {
		case SESSIONEXPIRED:
		case NOAUTH:
			dead = true;
			listener.onClose(code);
			return;
		default:
			break;
		}
	}

	@Override
	public void process(WatchedEvent event) {
		String path = event.getPath();
		EventType type = event.getType();
		// System.out.format("%s\t%s\t%s\r\n", path, state, type);
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
		} else if (path != null && path.equals(leaderAgentNode)) {
			if (type == EventType.NodeCreated) {

			} else if (type == EventType.NodeDeleted) {
				listener.onLeaderRemoved();
			}
			// Something has changed on the node, let's find out
			zk.exists(leaderAgentNode, this, this, null);
		}
	}
}
