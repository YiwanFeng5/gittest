package cn.fywspring.common;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import cn.fywspring.zebra_jobtracker.FileCollector;
import cn.fywspring.zebra_jobtracker.FileToBlock;
import cn.fywspring.zk.ZKConnectRunner;

public class Start {
	public static void main(String[] args) {
		System.out.println("jobtracker 启动了……");
		ExecutorService es = Executors.newCachedThreadPool();
		es.submit(new FileCollector());
		es.submit(new FileToBlock());
		es.submit(new ZKConnectRunner());
	}
}
