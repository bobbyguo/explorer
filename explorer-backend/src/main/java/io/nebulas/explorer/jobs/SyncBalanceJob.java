package io.nebulas.explorer.jobs;

import java.math.BigDecimal;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.joda.time.LocalDateTime;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import io.nebulas.explorer.domain.NebAddress;
import io.nebulas.explorer.service.blockchain.NebAddressService;
import io.nebulas.explorer.service.thirdpart.nebulas.NebApiServiceWrapper;
import io.nebulas.explorer.service.thirdpart.nebulas.bean.GetAccountStateResponse;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@AllArgsConstructor
@Component
public class SyncBalanceJob {

	private final NebAddressService nebAddressService;
    private final NebApiServiceWrapper nebApiServiceWrapper;
    boolean isRuning = false;
	@Scheduled(cron = "0 0/30 * * * ?")
	public void sync() {
		try {
			if (isRuning) return;
			else isRuning = true;
			int page = 1;
			int pageSize = 500;
			boolean loop = true;
			do {
				List<NebAddress> addresses = nebAddressService.findAddressOrderByBalance(page++, pageSize);
				loop = addresses.size() == pageSize;
				addresses.parallelStream().forEach(address -> {
					if (address.getUpdatedAt().before(LocalDateTime.now().plusSeconds(-5).toDate())) {
						GetAccountStateResponse accountState = nebApiServiceWrapper.getAccountState(address.getHash());
						if (null != accountState && StringUtils.isNotEmpty(accountState.getBalance())) {
							String balance = accountState.getBalance();
							address.setCurrentBalance(new BigDecimal(balance));
							nebAddressService.updateAddressBalance(address.getHash(), balance);
						}
					}	
				});
			} while(loop);
		} finally {
			isRuning = false;
		}

	}
}
