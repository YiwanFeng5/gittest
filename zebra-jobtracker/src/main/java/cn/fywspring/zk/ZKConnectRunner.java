package cn.fywspring.zk;

import java.util.List;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;

import cn.fywspring.rpc.RPCClientRunner;
import cn.fywspring.zebra.common.GlobalEnv;

public class ZKConnectRunner implements Runnable {
	private ZooKeeper zk;
	@Override
	public void run() {
		try {
			zk = GlobalEnv.connectZkServer();
			//获取/engine1下的所有子节点
			List<String> childPaths = zk.getChildren(GlobalEnv.getEngine1path(), null);
			for (String childPath : childPaths) {
				//启动一个发送FileSplit线程
				new Thread(new RPCClientRunner(zk, childPath)).start();
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		
	}
	
}
