package cn.fywspring.rpc;

import java.util.Map;

import org.apache.avro.AvroRemoteException;

import cn.fywspring.common.OwnEnv;
import cn.fywspring.reducer.ReducerRunner;
import rpc.domain.HttpAppHost;
import rpc.service.RpcSendHttpAppHost;

public class RpcSendHttpAppHostImpl implements RpcSendHttpAppHost{
	public Void sendHttpAppHost(HttpAppHost httpAppHost) throws AvroRemoteException {
		return null;
	}
	public Void sendHttpAppHostMap(Map<CharSequence, HttpAppHost> hahMap) throws AvroRemoteException {
		System.out.println("二级引擎接收到一级引擎传递过来的map"+hahMap.size());
		OwnEnv.getMapQueue().add(hahMap);
		//一级引擎处理完后，二级引擎开始归并
		//作业：切片后jobtracker项目项目创建/jobtracker节点，值：4
		//从/jobtracker节点上获取该节点的值
		if(OwnEnv.getMapQueue().size()==4){
			//启动一个归并的线程，来进行合并
			new Thread(new ReducerRunner()).start();
		}
		return null;
	}

}
