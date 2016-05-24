package bq.demo.zk;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Executors;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooKeeper.States;
import org.apache.zookeeper.data.ACL;

/**
 * 
 * 
 * After stop 1 zk server:

-----Got Event-----
path:null
type: name:None/value:-1
state: name:Disconnected/value:0

-----Got Event-----
path:null
type: name:None/value:-1
state: name:SyncConnected/value:3

-----Got Event-----
path:null
type: name:None/value:-1
state: name:Disconnected/value:0

-----Got Event-----
path:null
type: name:None/value:-1
state: name:Expired/value:-112

 * 
 * @author qibo
 *
 */
public class KeepWatching {
	
	private ZooKeeper zk;
	
	public static void main(String[] args) throws IOException, KeeperException, InterruptedException {
		KeepWatching kw = new KeepWatching();
		kw.init();
		
		Executors.newSingleThreadExecutor().execute(()->kw.run());
	}
	
	public void init() throws IOException, KeeperException, InterruptedException{
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
		
		zk = new ZooKeeper(connectString, 3000, watcher);
		
		List<ACL> acls = Arrays.asList(new ACL(ZooDefs.Perms.ALL,ZooDefs.Ids.ANYONE_ID_UNSAFE));
		
		zk.delete("/zk_boqi_test", -1);
		zk.create("/zk_boqi_test", "Boqi has this lock!!".getBytes(), acls, CreateMode.PERSISTENT);
	}
	
	public void run(){
		try {
            synchronized (this) {
            	States state = zk.getState();
				while(state != null){
            		System.out.println("...isAlive: " + state.isAlive() + " | isconnectd: " + state.isConnected());
					try {
						byte[] data = zk.getData("/zk_boqi_test", false, null);
						System.out.println("data: " + new String(data));
					} catch (KeeperException e) {
						System.out.println("## zk getdata exception: " + e.getMessage());
					}
                    wait(1000);
            	}
            	
            	System.out.println("### all zk servers down.. exit");
            }
        } catch (InterruptedException e) {
		}
	}
	
}
