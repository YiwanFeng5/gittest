package cn.fywspring.common;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import rpc.domain.HttpAppHost;

/**
 * @author jinxf
 *
 */
public class OwnEnv {
	private static BlockingQueue<Map<CharSequence,HttpAppHost>> mapQueue=new LinkedBlockingQueue<>();
	public static BlockingQueue<Map<CharSequence, HttpAppHost>> getMapQueue() {
		return mapQueue;
	}
	public static void setMapQueue(BlockingQueue<Map<CharSequence, HttpAppHost>> mapQueue) {
		OwnEnv.mapQueue = mapQueue;
	}
}
