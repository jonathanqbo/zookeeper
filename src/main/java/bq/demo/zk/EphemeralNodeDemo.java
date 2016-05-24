package bq.demo.zk;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;
import org.junit.Assert;
import org.junit.Test;

public class EphemeralNodeDemo {
	
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
		
		List<ACL> acls = Arrays.asList(new ACL(ZooDefs.Perms.ALL,ZooDefs.Ids.ANYONE_ID_UNSAFE));
		
		String path = zk.create("/zk_boqi_ephemeral", "Boqi has this lock!!".getBytes(), acls, CreateMode.EPHEMERAL);
		System.out.println("Ephemeral node created at path " + path);
		
		Stat exists = zk.exists("/zk_boqi_ephemeral", watcher);
		assertNotNull(exists);
		byte[] data = zk.getData("/zk_boqi_ephemeral", watcher, null);
		System.out.println("### get node data: " + new String(data, StandardCharsets.UTF_8));
		
		zk.close();
		
		zk = new ZooKeeper(connectString, 3000, watcher);
		exists = zk.exists("/zk_boqi_ephemeral", watcher);
		assertNull(exists);
		
		System.out.println("### after disconnection, ephemeral node gone!");
		
		zk.close();
	}
	
}
