package io.nebulas.explorer.jobs;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import io.nebulas.explorer.task.DataInitTask;

@Component
public class SyncDataJob {

	@Autowired
	private DataInitTask dataInitTask;
	
    @Scheduled(cron = "* */1 * * * ?")
    public void sync() {
    	dataInitTask.init(true);
    }
}
