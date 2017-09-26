package cn.fywspring.common;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import cn.fywspring.rpc.RpcServerRunner;
import cn.fywspring.zk.ZkConectRunner;

public class Start {
	public static void main(String[] args) {
		System.out.println("二级引擎启动了");
		ExecutorService es = Executors.newCachedThreadPool();
		es.submit(new ZkConectRunner());
		es.submit(new RpcServerRunner());
	}
}
