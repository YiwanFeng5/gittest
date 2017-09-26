package cn.fywspring.zebra_jobtracker;

import java.io.File;

import cn.fywspring.zebra.common.GlobalEnv;
/**
 * 定期扫描指定的目录，拿到日志， 然后将日志文件保存到队列中，等待后续处理
 * @author Yiwan
 *
 */
public class FileCollector implements Runnable {

	@Override
	public void run() {
		try {
			while(true) {
				//收集日志文件
				//获取全部变量中日志文件的目录dir
				File dirFile = new File(GlobalEnv.getDir());
				//获取日志目录下所有的日志文件
				File[] files = dirFile.listFiles();
				//遍历获取未处理的日志文件
				for (File file : files) {
					//如果后缀名为.ctr，同名的日志文件未处理的日志文件
					if (file.getName().endsWith(".ctr")) {
						String csvName = file.getName().split(".ctr")[0] + ".csv";
						File logFile = new File(GlobalEnv.getDir(),csvName);
						//将日志文件添加到队列中，等待后续处理
						GlobalEnv.getFileQueue().add(logFile);
						//删除标志文件
						file.delete();
					}
				}
				//每隔指定时间收集一次日志
				Thread.sleep(GlobalEnv.getScannningInterval());
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
}
