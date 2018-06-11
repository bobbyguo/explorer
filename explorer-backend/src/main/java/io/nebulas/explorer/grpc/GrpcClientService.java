package io.nebulas.explorer.grpc;

import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Service;

import com.alibaba.fastjson.JSONObject;

import io.grpc.Channel;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import io.nebulas.explorer.domain.NebBlock;
import io.nebulas.explorer.service.blockchain.NebBlockService;
import io.nebulas.explorer.service.blockchain.NebSyncService;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import rpcpb.ApiServiceGrpc;
import rpcpb.Rpc;

/**
 * Title.
 * <p>
 * Description.
 *
 * @author Bill
 * @version 1.0
 * @since 2018-01-23
 */
@Slf4j(topic = "subscribe")
@AllArgsConstructor
@Service
public class GrpcClientService {
    private GrpcChannelService grpcChannelService;
    private NebBlockService nebBlockService;
    private NebSyncService nebSyncService;

//    private static ExecutorService LINK_BLOCK_EXECUTOR = Executors.newFixedThreadPool(5);
//    private static ExecutorService PENDING_TX_EXECUTOR = Executors.newFixedThreadPool(20);
//    private static ExecutorService LIB_BLOCK_EXECUTOR = Executors.newFixedThreadPool(1);
    private static final int CPU_CORE = Runtime.getRuntime().availableProcessors();
	private static final ThreadPoolExecutor EXECUTOR = new ThreadPoolExecutor(CPU_CORE * 2, CPU_CORE * 2, 1, TimeUnit.HOURS,
			new ArrayBlockingQueue<Runnable>(1000), new ThreadPoolExecutor.CallerRunsPolicy());

    public void subscribe() {
        Channel channel = grpcChannelService.getChannel();

        ApiServiceGrpc.ApiServiceStub asyncStub = ApiServiceGrpc.newStub(channel);
        final CountDownLatch finishLatch = new CountDownLatch(1);

        StreamObserver<Rpc.SubscribeResponse> responseObserver = new StreamObserver<Rpc.SubscribeResponse>() {
            @Override
            public void onNext(Rpc.SubscribeResponse sr) {
                String dataStr = sr.getData();
                log.info("msg type: {}, data: {}", sr.getTopic(), dataStr);

                if (StringUtils.isBlank(dataStr)) {
                    log.error("empty data");
                    return;
                }

                JSONObject data;
                try {
                    data = JSONObject.parseObject(dataStr);
                } catch (Throwable e) {
                    log.error(String.format("data string %s can NOT parse into json", dataStr), e);
                    return;
                }

                String topic = sr.getTopic();
                String hash = data.getString("hash");

                if (Const.TopicLinkBlock.equals(topic)) {
                    EXECUTOR.execute(() -> processTopicLinkBlock(hash));
                } else if (Const.TopicPendingTransaction.equals(topic)) {
                	//ignore pending tx for performance
                    //PENDING_TX_EXECUTOR.execute(() -> processTopicPendingTransaction(hash));
                } else if (Const.TopicLibBlock.equals(topic)) {
                    EXECUTOR.execute(() -> processTopicLibBlock(hash));
                }
            }

            @Override
            public void onError(Throwable t) {
                Status status = Status.fromThrowable(t);
                log.warn("failed: {}", status);
                try {
                    grpcChannelService.renewChannel();
                    // sleep for 10 seconds to enter next retry
                    log.info("entering next retry after 10 seconds ....");
                    TimeUnit.SECONDS.sleep(10);
                    subscribe();
                } catch (InterruptedException e1) {
                    log.error("thread sleep interrupted, skipped reconnect", e1);
                    finishLatch.countDown();
                }
            }

            @Override
            public void onCompleted() {
                log.info("Finished");
                finishLatch.countDown();
            }
        };

        asyncStub.subscribe(Rpc.SubscribeRequest.newBuilder()
//                .addTopics(Const.TopicPendingTransaction)
                .addTopics(Const.TopicLinkBlock)
                .addTopics(Const.TopicLibBlock)
                .build(), responseObserver);
    }

    private void processTopicPendingTransaction(String hash) {
        if (StringUtils.isBlank(hash)) {
            log.error("empty hash");
            return;
        }
        try {
            nebSyncService.syncPendingTx(hash);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }

    private void processTopicLinkBlock(String hash) {
        if (StringUtils.isBlank(hash)) {
            log.error("empty hash");
            return;
        }
        try {
            NebBlock nebBlock = nebBlockService.getNebBlockByHash(hash);
            if (null != nebBlock) {
                log.warn("block with hash {} already existed", hash);
                return;
            }

            nebSyncService.syncBlockByHash(hash, false);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }

    private void processTopicLibBlock(String hash) {
        try {
            NebBlock nebBlock = nebBlockService.getNebBlockByHash(hash);
            if (null == nebBlock) {
                log.info("block with hash {} has not been synced", hash);
                nebSyncService.syncBlockByHash(hash, true);//height 小于他的块交由下次lib事件处理
            } else {
                List<NebBlock> nebBlockList = nebBlockService.findUnLibBlockLessThanHeight(nebBlock.getHeight(), 30);
                for (NebBlock blk : nebBlockList) {
                    nebSyncService.syncBlockByHeight(blk.getHeight(), true);
                }
            }
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }

}
