package cn.fywspring.zebra_jobtracker;

import java.io.File;

import cn.fywspring.zebra.common.GlobalEnv;
import rpc.domain.FileSplit;

/**
 * 这是一个线程类，作用是：
 * 1.从FileQueue队列中拿出日志文件，然后做逻辑切块
 * 切块分为两种：
 * 物理切块：真切，即生成切块文件
 * 逻辑切块：假切，不生成切块文件
 * 10MB：0,3MB | 3MB，6MB | 6MB，9MB | 9MB，10MB
 * 2.切块后封装到FileSplit对象中，然后将对象保存到队列里等待后续处理
 * @author Yiwan
 *
 */
public class FileToBlock implements Runnable {

	@Override
	public void run() {
		try {
			while(true) {
				//从队列中获取日志文件 take():如果没有日志文件则阻塞
				File file = GlobalEnv.getFileQueue().take();
				long length = file.length();
				//计算切块数量
				long num = length%GlobalEnv.getBlocksize() == 0 ? length/GlobalEnv.getBlocksize() : length/GlobalEnv.getBlocksize()+1;
				//遍历封装FileSplit对象
				for(int i = 0; i< num; i++) {
					FileSplit split = new FileSplit();
					split.setPath(file.getPath());
					split.setStart(i * GlobalEnv.getBlocksize());
					if (i == num-1) {//最后一块
						split.setLength(length-split.getStart());
					} else {
						split.setLength(GlobalEnv.getBlocksize());
					}
					//TODO 将切块保存到对应的队列中
					GlobalEnv.getSplitQueue().add(split);
					//输出切块信息
					System.out.println(split);
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
