package cn.fywspring.rpc;

import org.apache.avro.AvroRemoteException;

import cn.fywspring.common.OwnEnv;
import rpc.domain.FileSplit;
import rpc.service.RpcFileSplit;

public class RPCFileSplitImpl implements RpcFileSplit {

	@Override
	public Void sendFileSplit(FileSplit fileSplit) throws AvroRemoteException {
		System.out.println("一级引擎接受到了 jobtracker 发过来的"+fileSplit);
		//将之保存到一级引擎队列中
		OwnEnv.getSpiltQueue().add(fileSplit);
		return null;
	}
	
}
