package cn.fywspring.common;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import cn.fywspring.mapper.MapperRunner;
import cn.fywspring.rpc.RPCClientRunner;
import cn.fywspring.rpc.RPCServerRunner;
import cn.fywspring.zk.ZKConnectRunner;

public class Start {
	public static void main(String[] args) {
		System.out.println("一级引擎01节点启动了");
		ExecutorService es = Executors.newCachedThreadPool();
		es.submit(new ZKConnectRunner());
		es.submit(new RPCServerRunner());
		es.submit(new MapperRunner());
		es.submit(new RPCClientRunner());
	}
}
