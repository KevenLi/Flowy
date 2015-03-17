
import java.io.IOException;

import junit.framework.TestCase;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.junit.Test;

public class CreateTaskTest extends TestCase {

	public static void main(String[] args) {
		try {
			ZooKeeper zk = new ZooKeeper("localhost", 3000, null);

			String root = "/flowy/taskservers/127.0.0.1/tasks";
			if (zk.exists(root, false) == null) { 
				zk.create(root, new byte[0], Ids.OPEN_ACL_UNSAFE,
						CreateMode.PERSISTENT);
			}

			String task_node = root + "/1";
			if (zk.exists(task_node, null) != null) {
				for (String childNode : zk.getChildren(task_node, false)){
					zk.delete(task_node + "/" + childNode, -1);
				}
				zk.delete(task_node, -1);
			}

			String config = "package_url=http://192.168.105.122:8080/v1/AUTH_test/test2/flowy/agent/tasks/Flowy.TaskRuntime.zip\r\n"
					+ "runtime_config_url=http://192.168.105.122:8080/v1/AUTH_test/test2/flowy/agent/tasks/runtime.config\r\n"
					+ "task_config_url=http://192.168.105.122:8080/v1/AUTH_test/test2/flowy/agent/tasks/task.config\r\n"
					+ "runtime_type=python";
			byte[] data = config.getBytes();

			zk.create(task_node, data, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
			zk.close();
		} catch (KeeperException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	@Test
    public void testStartTask() throws IOException, InterruptedException, KeeperException {
		String zkhosts = "localhost";
		String taskServer = "127.0.0.1";
		String task = "1";
		ZooKeeper zk = new ZooKeeper(zkhosts, 3000, null);

		String root = String.format("/flowy/taskservers/%s/tasks/%s", taskServer, task);
		String startNode = root + "/start";
		if (zk.exists(startNode, false)!= null){
			zk.delete(startNode, -1);
		}
		zk.create(startNode, new byte[0], Ids.OPEN_ACL_UNSAFE,
					CreateMode.PERSISTENT);
		
		
		zk.close();
    }
}
