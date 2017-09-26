package cn.fywspring.rpc;


import java.net.InetSocketAddress;
import java.util.concurrent.CountDownLatch;

import org.apache.avro.AvroRemoteException;
import org.apache.avro.ipc.NettyTransceiver;
import org.apache.avro.ipc.specific.SpecificRequestor;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.ZooKeeper;

import cn.fywspring.zebra.common.GlobalEnv;
import rpc.domain.FileSplit;
import rpc.service.RpcFileSplit;

public class RPCClientRunner implements Runnable {

	private ZooKeeper zk;
	private String childPath;
	
	public RPCClientRunner(ZooKeeper zk, String childPath) {
		this.zk = zk;
		this.childPath = childPath;
	}

	@Override
	public void run() {
		try {
			System.out.println("childPath: " + childPath);
			byte[] data = zk.getData(GlobalEnv.getEngine1path()+"/"+childPath,null,null);
			String info = new String(data);
			String ip = info.split("/")[1];
			int port = Integer.parseInt(info.split("/")[2]);
			NettyTransceiver client = new NettyTransceiver(new InetSocketAddress(ip, port));
			final RpcFileSplit proxy = SpecificRequestor.getClient(RpcFileSplit.class, client);
			
			//仅仅发送一次
			FileSplit fileSplit = GlobalEnv.getSplitQueue().take();
			//发送
			proxy.sendFileSplit(fileSplit);
			
			//一直监听对应的节点的状态变化
			while(true) {
				final CountDownLatch cdl = new CountDownLatch(1);
				zk.getData(GlobalEnv.getEngine1path()+"/"+childPath,new Watcher() {
					
					@Override
					public void process(WatchedEvent event) {
						if (event.getType() == EventType.NodeDataChanged) {
							try {
								byte[] data = zk.getData(GlobalEnv.getEngine1path()+"/"+childPath, null, null);
								//判断状态是否为free
								if (new String(data).endsWith("free")) {
									//获取切片对象
									FileSplit split = GlobalEnv.getSplitQueue().poll();
									if (split != null) {
										proxy.sendFileSplit(split);
										cdl.countDown();
									}
								} else {
									cdl.countDown();
								}
							} catch (KeeperException | InterruptedException e) {
								e.printStackTrace();
							} catch (AvroRemoteException e) {
								e.printStackTrace();
							}
						}
					}
				} , null);
				cdl.await();
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
