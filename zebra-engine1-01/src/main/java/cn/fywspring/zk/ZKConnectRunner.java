package cn.fywspring.zk;


import java.net.InetAddress;
import java.net.UnknownHostException;

import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;

import cn.fywspring.common.OwnEnv;
import cn.fywspring.zebra.common.GlobalEnv;
/**
 * 这个线程类实现将一级引擎在zk服务器上创建一个节点
 * 并将ip和port保存到节点中
 * @author Yiwan
 *
 */
public class ZKConnectRunner implements Runnable {

	private static ZooKeeper zk;
	
	@Override
	public void run() {
		try {
			zk = GlobalEnv.connectZkServer();
			String info = InetAddress.getLocalHost().toString()+"/"+OwnEnv.getRpcport()+"/free";
			//创建node节点
			zk.create(GlobalEnv.getEngine1path()+OwnEnv.getZnodepath(), info.getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public static void main(String[] args) throws UnknownHostException {
		System.out.println(InetAddress.getLocalHost().toString());
	}
	
	public static void setBusy() throws Exception {
		String info = InetAddress.getLocalHost().toString()+"/"+OwnEnv.getRpcport()+"/busy";
		zk.setData(GlobalEnv.getEngine1path()+OwnEnv.getZnodepath(), info.getBytes(), -1);
	}
	public static void setFree() throws Exception {
		String info = InetAddress.getLocalHost().toString()+"/"+OwnEnv.getRpcport()+"/free";
		zk.setData(GlobalEnv.getEngine1path()+OwnEnv.getZnodepath(), info.getBytes(), -1);
	}
}
