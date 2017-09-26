package cn.fywspring.rpc;

import java.net.InetSocketAddress;

import org.apache.avro.ipc.NettyServer;
import org.apache.avro.ipc.specific.SpecificResponder;

import cn.fywspring.common.OwnEnv;
import cn.fywspring.zebra.common.GlobalEnv;
import rpc.service.RpcFileSplit;

public class RPCServerRunner implements Runnable {

	@Override
	public void run() {
		try {
			NettyServer server = new NettyServer(new SpecificResponder(RpcFileSplit.class, new RPCFileSplitImpl()), new InetSocketAddress(OwnEnv.getRpcport()));
		} catch (Exception e) {
			e.printStackTrace();
		}
		
	}
	
}
