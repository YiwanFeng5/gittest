package cn.fywspring.zk;

import java.net.InetAddress;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooDefs.Ids;

import cn.fywspring.zebra.common.GlobalEnv;

public class ZkConectRunner implements Runnable{
	private ZooKeeper zk;
	public void run() {
		try {
			zk = GlobalEnv.connectZkServer();
			String info = InetAddress.getLocalHost().toString()+"/8888";
			//创建/engine2临时节点
			zk.create(GlobalEnv.getEngine2path(), info.getBytes(), 
					Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
