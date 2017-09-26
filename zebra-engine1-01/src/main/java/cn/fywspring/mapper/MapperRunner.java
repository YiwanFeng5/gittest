package cn.fywspring.mapper;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.HashMap;
import java.util.Map;

import cn.fywspring.common.OwnEnv;
import cn.fywspring.zk.ZKConnectRunner;
import rpc.domain.FileSplit;
import rpc.domain.HttpAppHost;

public class MapperRunner implements Runnable {

	@Override
	public void run() {
		try {
			while(true) {
				//保存切块内容处理合并后的信息（后添加）
				Map<CharSequence, HttpAppHost> map = new HashMap<>();
				FileSplit split = OwnEnv.getSpiltQueue().take();
				//将切点状态改为busy
				ZKConnectRunner.setBusy();
				long start = split.getStart();
				long end = start + split.getLength();
				File file = new File(split.getPath().toString());
				//有可能切块后，考虑到不一定都是从行首、行尾切
				//所以需要追溯到start和end，均采用向前追溯的方式
				FileInputStream in = new FileInputStream(file);
				FileChannel fc = in.getChannel();
				if(start != 0) {
					long headPosition = start;
					while(true) {
						ByteBuffer buff = ByteBuffer.allocate(1);
						fc.position(headPosition);
						fc.read(buff);
						if (new String(buff.array()).equals("\n")) {
							start = headPosition+1;
							break;
						} else {
							headPosition--;
						}
					}
				}
				if (end != file.length()) {
					long tailPosition = end;
					while(true) {
						ByteBuffer buf = ByteBuffer.allocate(1);
						fc.position(tailPosition);
						fc.read(buf);
						if (new String(buf.array()).equals("\n")) {
							end = tailPosition;
							break;
						} else {
							tailPosition--;
						}
					}
				}
				//start 和 end 处理完后，读取本次切块的所有行
				ByteBuffer buffer = ByteBuffer.allocate((int)(end-start));
				fc.position(start);
				fc.read(buffer);
				//一行行读取
				BufferedReader br = new BufferedReader(new InputStreamReader(new ByteArrayInputStream(buffer.array())));
				String line = null;
				while((line=br.readLine()) != null) {
					String data[] = line.split("\\|");
					//一下代码从业务说明文档的业务字段处理逻辑处拷贝
					HttpAppHost hah=new HttpAppHost();
					String reportTime=file.getPath().toString().split("_")[1];
					hah.setReportTime(reportTime);
					
					//上网小区的id
					hah.setCellid(data[16]);
					//应用大类
					hah.setAppType(Integer.parseInt(data[22]));
					//应用子类
					hah.setAppSubtype(Integer.parseInt(data[23]));
					//用户ip
					hah.setUserIP(data[26]);
					//用户port
					hah.setUserPort(Integer.parseInt(data[28]));
					//访问的服务ip
					hah.setAppServerIP(data[30]);
					//访问的服务port
					hah.setAppServerPort(Integer.parseInt(data[32]));
					//域名
					hah.setHost(data[58]);
					int appTypeCode=Integer.parseInt(data[18]);
					String transStatus=data[54];
					//业务逻辑处理
					if(hah.getCellid()==null||hah.getCellid().equals("")){
					hah.setCellid("000000000");
					}
					//如果状态码103，就把尝试请求次数设为1
					if(appTypeCode==103){
					hah.setAttempts(1);
					}
					//如果状态码103，并且传输码包括这么多……，就把接收次数设置为1
					if(appTypeCode==103 &&"10,11,12,13,14,15,32,33,34,35,36,37,38,48,49,50,51,52,53,54,55,199,200,201,202,203,204,205 ,206,302,304,306".contains(transStatus)){
					hah.setAccepts(1);
					}else{
					hah.setAccepts(0);
					}
					//如果是103，就设置用户发生的上传流量，后续后统计每个用户产生的总的上传流量
					if(appTypeCode == 103){
					hah.setTrafficUL(Long.parseLong(data[33]));
					}
					//如果是103,设置下行流量
					if(appTypeCode == 103){
					hah.setTrafficDL(Long.parseLong(data[34]));
					}
					//如果是103，设置重传上行流量
					if(appTypeCode == 103){
						hah.setRetranUL(Long.parseLong(data[39]));
					}
					//如果是103，设置重传下行流量
					if(appTypeCode == 103){
						hah.setRetranDL(Long.parseLong(data[40]));
					}
					//如果是103,设置用户的传输延迟
					if(appTypeCode==103){
						hah.setTransDelay(Long.parseLong(data[20]) -Long.parseLong(data[19]));
					}
					//标识用户的key
					CharSequence key=hah.getReportTime() + "|" + hah.getAppType() + "|" + hah.getAppSubtype() + "|" + hah.getUserIP() + "|" + hah.getUserPort() + "|" + hah.getAppServerIP() + "|" + hah.getAppServerPort() +"|" + hah.getHost() + "|" + hah.getCellid();
					//map（）
					//用户甲：
					//hah=>map
					//重点理解好。
					if(map.containsKey(key)){
						HttpAppHost mapHah=map.get(key);
						mapHah.setAccepts(mapHah.getAccepts()+hah.getAccepts());
						mapHah.setAttempts(mapHah.getAttempts()+hah.getAttempts());
						mapHah.setTrafficUL(mapHah.getTrafficUL()+hah.getTrafficUL());
						mapHah.setTrafficDL(mapHah.getTrafficDL()+hah.getTrafficDL());
						mapHah.setRetranUL(mapHah.getRetranUL()+hah.getRetranUL());
						mapHah.setRetranDL(mapHah.getRetranDL()+hah.getRetranDL());
						mapHah.setTransDelay(mapHah.getTransDelay()+hah.getTransDelay());
						map.put(key, mapHah);
					}else{
						map.put(key,hah);
					}
					//拷贝结束
				}
				//将map保存到队列中
				OwnEnv.getMapQueue().add(map);
				//将节点状态改为free
				ZKConnectRunner.setFree();
				System.out.println(map.size());
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
}
