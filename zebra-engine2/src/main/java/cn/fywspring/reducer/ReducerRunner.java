package cn.fywspring.reducer;

import java.util.HashMap;
import java.util.Map;

import cn.fywspring.common.OwnEnv;
import cn.fywspring.zebra.db.ZebraDB;
import rpc.domain.HttpAppHost;

public class ReducerRunner implements Runnable{
	private Map<String,HttpAppHost> map= new HashMap<>();
	public void run() {
		while(true){
			Map<CharSequence,HttpAppHost> reducerMap = OwnEnv.getMapQueue().poll();
			if(reducerMap==null){
				//所有的map都归并完了，跳转循环
				System.out.println("归并结束，跳出循环");
				break;
			}
			for (Map.Entry<CharSequence, HttpAppHost> entry :reducerMap.entrySet()) {
				//获取用户标识
				String key = entry.getKey().toString();
				HttpAppHost hah = entry.getValue();
				//判断map中是否存在key
				if(map.containsKey(key)){
					HttpAppHost mapHah=map.get(key);
					mapHah.setAccepts(mapHah.getAccepts()+hah.getAccepts());
					mapHah.setAttempts(mapHah.getAttempts()+hah.getAttempts());
					mapHah.setTrafficUL(mapHah.getTrafficUL()+hah.getTrafficUL());
					mapHah.setTrafficDL(mapHah.getTrafficDL()+hah.getTrafficDL());
					mapHah.setRetranUL(mapHah.getRetranUL()+hah.getRetranUL());
					mapHah.setRetranDL(mapHah.getRetranDL()+hah.getRetranDL());
					mapHah.setTransDelay(mapHah.getTransDelay()+hah.getTransDelay());
					//map.put(key, mapHah);//?
				}else{
					map.put(key, hah);
				}
			}
		}
		//归并完成了
		System.out.println("归并后map"+map.size());
		//数据落地
		ZebraDB.toDb(map);
	}
}
