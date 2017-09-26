package cn.fywspring.rpc;

import java.net.InetSocketAddress;

import org.apache.avro.ipc.NettyTransceiver;
import org.apache.avro.ipc.specific.SpecificRequestor;
import org.apache.zookeeper.ZooKeeper;

import cn.fywspring.common.OwnEnv;
import cn.fywspring.zebra.common.GlobalEnv;
import rpc.service.RpcSendHttpAppHost;

public class RPCClientRunner implements Runnable {

	private ZooKeeper zk;
	
	@Override
	public void run() {
		try {
			//获取zk对象
			zk = GlobalEnv.connectZkServer();
			//获取engine2节点数据
			byte[] data = zk.getData(GlobalEnv.getEngine2path(), null, null);
			String info = new String(data);
			String ip = info.split("/")[1];
			int port = Integer.parseInt(info.split("/")[2]);
			NettyTransceiver client = new NettyTransceiver(new InetSocketAddress(ip, port));
			RpcSendHttpAppHost proxy = SpecificRequestor.getClient(RpcSendHttpAppHost.class, client);
			//向二级引擎传递处理后的map
			while(true) {
				proxy.sendHttpAppHostMap(OwnEnv.getMapQueue().take());
			}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 
	}

}
