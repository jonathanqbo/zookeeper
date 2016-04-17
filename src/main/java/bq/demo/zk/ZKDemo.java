package bq.demo.zk;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;
import org.junit.Test;

/**
 * ZK code available to use are all from two packages:
 * <li> org.apache.zookeeper
 * <li> org.apache.zookeeper.data
 * 
 * <p> so just need check these two package to find the API to use
 * 
 * @author qibo
 *
 */
public class ZKDemo {
	
	@Test
	public void test() throws IOException, KeeperException, InterruptedException{
		String connectString="192.168.0.32:2181,192.168.0.32:2182,192.168.0.32:2183";
		
		Watcher watcher = new Watcher(){

			@Override
			public void process(WatchedEvent event) {
				System.out.println("\n-----Got Event-----");
				System.out.println("path:" + event.getPath());
				System.out.println("type: name:" + event.getType().name() + "/value:" + event.getType().getIntValue());
				System.out.println("state: name:" + event.getState().name() + "/value:" + event.getState().getIntValue());
				System.out.println("\n");
			}
			
		};
		
		ZooKeeper zk = new ZooKeeper(connectString, 3000, watcher);
		
		// create znode
		Stat exists = zk.exists("/zk_boqi", true);
		List<ACL> acls = new ArrayList<>();
		acls.add(new ACL(ZooDefs.Perms.ALL,ZooDefs.Ids.ANYONE_ID_UNSAFE));
		
		if(exists == null){
			System.out.println("### node[zk_boqi] isn't existed");
			zk.create("/zk_boqi", "Boqi create me!!".getBytes(), acls, CreateMode.PERSISTENT);
		}
		
		// get data from created znode
		byte[] data = zk.getData("/zk_boqi", watcher, null);
		System.out.println("### Get data from znode[zk_boqi]:" + new String(data, StandardCharsets.UTF_8));
		
		// update data for created zonde
		exists = zk.exists("/zk_boqi", true);
		if(exists != null){
			System.out.println("### version before update: " + exists.getVersion());
			
			exists = zk.setData("/zk_boqi", "Boqi modified me!!".getBytes(), exists.getVersion());
			
			System.out.println("### version after update: " + exists.getVersion());
		}
		
		// delete znode
		zk.delete("/zk_boqi", exists.getVersion());
		
		exists = zk.exists("/zk_boqi", true);
		if(exists == null)
			System.out.println("### Boqi deleted me!!");
	}

}
